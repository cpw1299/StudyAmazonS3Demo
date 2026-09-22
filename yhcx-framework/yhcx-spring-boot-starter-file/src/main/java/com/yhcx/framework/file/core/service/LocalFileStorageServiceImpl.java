package com.yhcx.framework.file.core.service;

import com.amazonaws.services.s3.model.PartETag;
import com.yhcx.framework.file.config.YhcxFileProperty;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.multipart.MultipartFile;

/**
 * 本地文件分片上传服务实现
 *
 * @author liuhm
 * @version 1.0.0
 * @create 2026/8/11 18:52
 **/
@Slf4j
public class LocalFileStorageServiceImpl implements LocalFileStorageService {

    private static final String META_FILE = "upload.properties";
    private static final String PART_PREFIX = "part-";
    private static final int BUFFER_SIZE = 8192;

    private final Path uploadRoot;
    private final Path partRoot;

    /**
     * 正在执行合并(completeMultipartUpload)的 uploadId 标记。
     * 使用 CAS putIfAbsent 保证同一 uploadId 只有一个合并任务执行。
     */
    private final ConcurrentMap<String, Boolean> mergingStates = new ConcurrentHashMap<>();

    /**
     * 被取消/已完成的 uploadId 标记，防止 abort 后继续操作。
     */
    private final ConcurrentMap<String, Boolean> finalizedStates = new ConcurrentHashMap<>();

    public LocalFileStorageServiceImpl(YhcxFileProperty property) {
        this.uploadRoot = Paths.get(property.getUploadPath()).toAbsolutePath().normalize();
        this.partRoot = Paths.get(property.getPartPath()).toAbsolutePath().normalize();
        createDirectories(uploadRoot);
        createDirectories(partRoot);
    }

    @Override
    public String initiateMultipartUpload(String rootPath, String path, String contentType) {
        Path target = resolveTarget(rootPath, path);
        String uploadId = UUID.randomUUID().toString();
        Path uploadDirectory = resolveUploadDirectory(uploadId);
        try {
            Files.createDirectory(uploadDirectory);
            Properties properties = new Properties();
            properties.setProperty("target", uploadRoot.relativize(target).toString());
            if (contentType != null) {
                properties.setProperty("contentType", contentType);
            }
            try (OutputStream output = Files.newOutputStream(uploadDirectory.resolve(META_FILE),
                    StandardOpenOption.CREATE_NEW)) {
                properties.store(output, null);
            }
            return uploadId;
        } catch (IOException ex) {
            deleteDirectoryQuietly(uploadDirectory);
            throw storageException("初始化本地分片上传失败", ex);
        }
    }

    @Override
    public PartETag uploadMultipartPart(String rootPath, String path, String uploadId,
            Integer partNumber, MultipartFile file) throws IOException {
        checkNotFinalized(uploadId);
        checkNotMerging(uploadId);
        Path uploadDirectory = validateUpload(rootPath, path, uploadId);
        Path part = uploadDirectory.resolve(partFileName(partNumber));
        Path temporaryPart = uploadDirectory.resolve(partFileName(partNumber) + "." + UUID.randomUUID() + ".tmp");
        try {
            MessageDigest digest = MessageDigest.getInstance("MD5");
            try (InputStream input = new DigestInputStream(file.getInputStream(), digest)) {
                Files.copy(input, temporaryPart, StandardCopyOption.REPLACE_EXISTING);
            }
            moveReplacing(temporaryPart, part);
            return new PartETag(partNumber, toHex(digest.digest()));
        } catch (NoSuchAlgorithmException ex) {
            throw new IllegalStateException("当前运行环境不支持 MD5", ex);
        } finally {
            Files.deleteIfExists(temporaryPart);
        }
    }

    @Override
    public boolean checkUploadMultipartPart(String rootPath, String path, String uploadId,
            Integer partNumber) {
        Path uploadDirectory = validateUpload(rootPath, path, uploadId);
        return Files.isRegularFile(uploadDirectory.resolve(partFileName(partNumber)));
    }

    @Override
    public List<PartETag> listMultipartUploadParts(String rootPath, String path, String uploadId) {
        Path uploadDirectory = validateUpload(rootPath, path, uploadId);
        List<PartETag> parts = new ArrayList<>();
        try (DirectoryStream<Path> files = Files.newDirectoryStream(uploadDirectory, PART_PREFIX + "*")) {
            for (Path file : files) {
                Integer partNumber = parsePartNumber(file.getFileName().toString());
                if (partNumber != null && Files.isRegularFile(file)) {
                    parts.add(new PartETag(partNumber, calculateMd5(file)));
                }
            }
        } catch (IOException ex) {
            throw storageException("查询本地上传分片失败", ex);
        }
        parts.sort(Comparator.comparingInt(PartETag::getPartNumber));
        return parts;
    }

    @Override
    public String completeMultipartUpload(String rootPath, String path, String uploadId) {
        // 1. 检查 uploadId 是否已经被取消/完成
        if (finalizedStates.containsKey(uploadId)) {
            throw new IllegalStateException("本地分片上传任务已终止或已完成: " + uploadId);
        }
        log.debug("开始合并本地分片上传1: {}", uploadId);
        // 2. CAS 标记 MERGING 状态，防止并发重复合并
        //    putIfAbsent 返回 null 表示成功获取合并权，返回非 null 表示已存在正在合并的任务
        if (mergingStates.putIfAbsent(uploadId, Boolean.TRUE) != null) {
            throw new IllegalStateException("本地分片正在合并中，请勿重复提交: " + uploadId);
        }
        log.debug("开始合并本地分片上传2: {}", uploadId);
        try {
            // 3. 校验并读取分片信息（无锁，分片文件可与上传并发）
            Path uploadDirectory = validateUpload(rootPath, path, uploadId);
            List<PartETag> parts = listMultipartUploadParts(rootPath, path, uploadId);
            if (parts.isEmpty()) {
                throw new IllegalStateException("没有可合并的本地上传分片");
            }
            log.debug("开始合并本地分片上传3: {}", uploadId);
            validateContinuousParts(parts);
            log.debug("开始合并本地分片上传4: {}", uploadId);
            // 4. 执行文件合并（全在锁外，耗时 IO 不会阻塞其他请求）
            Path target = resolveTarget(rootPath, path);
            Path parent = target.getParent();
            Path temporaryTarget = parent.resolve(target.getFileName() + "." + uploadId + ".tmp");
            log.debug("开始合并本地分片上传5: {}{}", uploadId, temporaryTarget);
            try {
                Files.createDirectories(parent);
                try (OutputStream output = Files.newOutputStream(temporaryTarget,
                        StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
                    byte[] buffer = new byte[BUFFER_SIZE];
                    for (PartETag part : parts) {
                        try (InputStream input = Files.newInputStream(
                                uploadDirectory.resolve(partFileName(part.getPartNumber())))) {
                            copy(input, output, buffer);
                        }
                        log.debug("开始合并本地分片上传6，合并分片: {}{}", uploadId, part.getPartNumber());
                    }
                }
                moveReplacing(temporaryTarget, target);
                log.debug("开始合并本地分片上传7: {},替换文件", uploadId);
                deleteDirectory(uploadDirectory);
                // 5. 标记为已完成
                finalizedStates.put(uploadId, Boolean.TRUE);
                return target.toString();
            } catch (IOException ex) {
                // 合并失败清理临时文件
                try {
                    Files.deleteIfExists(temporaryTarget);
                } catch (IOException ignored) {
                }
                throw storageException("合并本地上传分片失败", ex);
            }
        } finally {
            // 清理 MERGING 标记（无论成功失败）
            mergingStates.remove(uploadId);
        }
    }

    @Override
    public void abortMultipartUpload(String rootPath, String path, String uploadId) {
        resolveUploadDirectory(uploadId);
        // 如果正在合并中，拒绝 abort（并发安全）
        if (mergingStates.containsKey(uploadId)) {
            throw new IllegalStateException("本地分片正在合并中，无法取消: " + uploadId);
        }
        // 如果已经被标记为终止/完成，直接返回（幂等）
        if (finalizedStates.putIfAbsent(uploadId, Boolean.TRUE) != null) {
            // 已存在，可能之前已被 abort 或 complete，尝试清理目录
            Path uploadDirectory = resolveUploadDirectory(uploadId);
            deleteDirectoryQuietly(uploadDirectory);
            return;
        }
        // 正常取消流程
        Path uploadDirectory = validateUpload(rootPath, path, uploadId);
        try {
            deleteDirectory(uploadDirectory);
        } catch (IOException ex) {
            // 标记已存在但文件删除失败，清理标记允许重试
            finalizedStates.remove(uploadId);
            throw storageException("取消本地分片上传失败", ex);
        }
    }

    private Path validateUpload(String rootPath, String path, String uploadId) {
        Path uploadDirectory = resolveUploadDirectory(uploadId);
        Path metadata = uploadDirectory.resolve(META_FILE);
        if (!Files.isRegularFile(metadata)) {
            throw new IllegalArgumentException("本地分片上传任务不存在: " + uploadId);
        }
        Properties properties = new Properties();
        try (InputStream input = Files.newInputStream(metadata)) {
            properties.load(input);
        } catch (IOException ex) {
            throw storageException("读取本地分片上传任务失败", ex);
        }
        String expectedTarget = uploadRoot.relativize(resolveTarget(rootPath, path)).toString();
        if (!expectedTarget.equals(properties.getProperty("target"))) {
            throw new IllegalArgumentException("上传任务与目标文件不匹配");
        }
        return uploadDirectory;
    }

    private Path resolveTarget(String rootPath, String path) {
        Path target = uploadRoot.resolve(rootPath).resolve(path).normalize();
        if (!target.startsWith(uploadRoot) || target.equals(uploadRoot)) {
            throw new IllegalArgumentException("文件上传路径不合法");
        }
        return target;
    }

    private Path resolveUploadDirectory(String uploadId) {
        try {
            UUID.fromString(uploadId);
        } catch (RuntimeException ex) {
            throw new IllegalArgumentException("uploadId 不合法", ex);
        }
        Path directory = partRoot.resolve(uploadId).normalize();
        if (!directory.getParent().equals(partRoot)) {
            throw new IllegalArgumentException("uploadId 不合法");
        }
        return directory;
    }

    /** 检查 uploadId 未处于终止/完成状态 */
    private void checkNotFinalized(String uploadId) {
        if (finalizedStates.containsKey(uploadId)) {
            throw new IllegalStateException("本地分片上传任务已终止或已完成: " + uploadId);
        }
    }

    /** 检查 uploadId 未处于合并中状态 */
    private void checkNotMerging(String uploadId) {
        if (mergingStates.containsKey(uploadId)) {
            throw new IllegalStateException("本地分片正在合并中，禁止上传新分片: " + uploadId);
        }
    }

    private static void validateContinuousParts(List<PartETag> parts) {
        for (int index = 0; index < parts.size(); index++) {
            if (parts.get(index).getPartNumber() != index + 1) {
                throw new IllegalStateException("上传分片编号不连续，缺少分片: " + (index + 1));
            }
        }
    }

    private static String partFileName(Integer partNumber) {
        if (partNumber == null || partNumber < 1 || partNumber > 10000) {
            throw new IllegalArgumentException("分片编号必须在 1 到 10000 之间");
        }
        return PART_PREFIX + partNumber;
    }

    private static Integer parsePartNumber(String fileName) {
        if (!fileName.startsWith(PART_PREFIX)) {
            return null;
        }
        try {
            return Integer.valueOf(fileName.substring(PART_PREFIX.length()));
        } catch (NumberFormatException ex) {
            return null;
        }
    }

    private static String calculateMd5(Path file) throws IOException {
        try {
            MessageDigest digest = MessageDigest.getInstance("MD5");
            byte[] buffer = new byte[BUFFER_SIZE];
            try (InputStream input = new DigestInputStream(Files.newInputStream(file), digest)) {
                while (input.read(buffer) != -1) {
                    // DigestInputStream 在读取时更新摘要
                }
            }
            return toHex(digest.digest());
        } catch (NoSuchAlgorithmException ex) {
            throw new IllegalStateException("当前运行环境不支持 MD5", ex);
        }
    }

    private static String toHex(byte[] bytes) {
        StringBuilder value = new StringBuilder(bytes.length * 2);
        for (byte item : bytes) {
            value.append(String.format("%02x", item & 0xff));
        }
        return value.toString();
    }

    private static void copy(InputStream input, OutputStream output, byte[] buffer) throws IOException {
        int length;
        while ((length = input.read(buffer)) != -1) {
            output.write(buffer, 0, length);
        }
    }

    private static void moveReplacing(Path source, Path target) throws IOException {
        try {
            Files.move(source, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (AtomicMoveNotSupportedException ex) {
            Files.move(source, target, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    private static void createDirectories(Path directory) {
        try {
            Files.createDirectories(directory);
        } catch (IOException ex) {
            throw storageException("创建本地文件存储目录失败: " + directory, ex);
        }
    }

    private static void deleteDirectory(Path directory) throws IOException {
        if (!Files.exists(directory)) {
            return;
        }
        try (Stream<Path> paths = Files.walk(directory)) {
            Path[] entries = paths.sorted(Comparator.reverseOrder()).toArray(Path[]::new);
            for (Path entry : entries) {
                Files.deleteIfExists(entry);
            }
        }
    }

    private static void deleteDirectoryQuietly(Path directory) {
        try {
            deleteDirectory(directory);
        } catch (IOException ignored) {
            // 初始化失败时尽力清理临时目录
        }
    }

    private static IllegalStateException storageException(String message, Exception cause) {
        return new IllegalStateException(message, cause);
    }
}
