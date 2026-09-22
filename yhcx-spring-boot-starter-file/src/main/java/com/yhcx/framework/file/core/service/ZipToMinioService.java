package com.yhcx.framework.file.core.service;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;
import com.yhcx.framework.common.util.file.Tools;
import com.yhcx.module.infra.framework.file.core.client.FileClient;
import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Objects;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException;
import java.util.zip.ZipFile;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ZIP 解压并上传 MinIO。
 *
 * <p>
 * 工作流程：
 *
 * <pre>
 * 本地 ZIP
 *    ↓
 * ZipFile
 *    ↓
 * ZipEntry
 *    ↓
 * InputStream
 *    ↓
 * AmazonS3Client
 *    ↓
 * MinIO
 * </pre>
 *
 * <p>
 * 注意：
 * 1. 不会将整个 ZIP 加载到内存
 * 2. 不会将解压后的文件落盘
 * 3. 大文件使用 Multipart Upload
 * 4. Multipart 使用固定大小 byte[]，避免内存随着文件大小增长
 */
public class ZipToMinioService {
    private final Logger log = LoggerFactory.getLogger(ZipToMinioService.class);
    /**
     * 小于该大小的 Entry，直接 PutObject。
     * <p>
     * 这里设置为 100MB。
     */
    private static final long DIRECT_UPLOAD_MAX_SIZE = 100L * 1024 * 1024;
    /**
     * Multipart Part 大小。
     * <p>
     * 64MB 是比较稳妥的生产配置。
     * <p>
     * 注意：
     * AWS S3 Multipart Upload 要求除最后一个 Part 外，
     * 每个 Part 至少 5MB。
     */
    private static final int PART_SIZE = 64 * 1024 * 1024;
    /**
     * 读取 ZIP Entry 时使用的 InputStream Buffer。
     */
    private static final int INPUT_BUFFER_SIZE = 1024 * 1024;

    /**
     * 尝试的zip文件名编码优先级列表：UTF‑8 -> GB18030 -> GBK
     */
    private static final List<Charset> ZIP_CHARSET_TRY_LIST = List.of(
            Charset.forName("UTF-8"),
            Charset.forName("GB18030"),
            Charset.forName("GBK")
    );

    private final AmazonS3Client amazonS3Client;

    public ZipToMinioService(FileClient fileClient) {
        this.amazonS3Client = (AmazonS3Client) Objects.requireNonNull(fileClient).getClient();
    }

    /**
     * 解压 ZIP，并将其中的文件上传到 MinIO。
     *
     * @param zipPath      Java 服务器上的 ZIP 文件
     * @param bucketName   MinIO Bucket
     * @param targetPrefix MinIO Object 前缀
     * @param deleteFlag   删除本地 ZIP 文件
     * @return 解压上传结果（文件数量、文件总大小）
     */
    public ExtractResult extractAndUpload(Path zipPath, String bucketName, String targetPrefix, boolean deleteFlag) throws IOException {
        return extractAndUpload(zipPath, bucketName, targetPrefix, deleteFlag, null);
    }

    /**
     * 解压 ZIP，并将其中的文件上传到 MinIO。
     *
     * @param zipPath            Java 服务器上的 ZIP 文件
     * @param bucketName         MinIO Bucket
     * @param targetPrefix       MinIO Object 前缀
     * @param deleteFlag         删除本地 ZIP 文件
     * @param uploadedObjectKeys 本次成功上传的对象 key 列表（可为 null；失败回滚时用于删除）
     * @return 解压上传结果（文件数量、文件总大小）
     */
    public ExtractResult extractAndUpload(Path zipPath, String bucketName, String targetPrefix,
                                          boolean deleteFlag, List<String> uploadedObjectKeys) throws IOException {
        checkZipFile(zipPath);
        long zipSize = Files.size(zipPath);
        log.info("ZIP 解压上传开始, zipPath={}, zipSize={}, bucket={}, prefix={}", zipPath, zipSize, bucketName, targetPrefix);
        long fileCount = 0;
        long totalBytes = 0;
        List<String> uploadedKeys = uploadedObjectKeys != null ? uploadedObjectKeys : new ArrayList<>();

        // -------- 修改点：不再硬编码GB18030，调用尝试多编码打开zip --------
        try (ZipFile zipFile = openZipWithEncodingTry(zipPath)) {
            Enumeration<? extends ZipEntry> entries = zipFile.entries();
            while (entries.hasMoreElements()) {
                ZipEntry entry = entries.nextElement();
                // 目录不需要上传
                if (entry.isDirectory()) {
                    continue;
                }
                String entryName = entry.getName();
                validateEntryName(entryName);
                long entrySize = entry.getSize();
                if (entrySize < 0) {
                    throw new IOException("ZIP Entry 大小未知: " + entryName);
                }
                String objectName = Tools.join(false, targetPrefix, entryName);
                log.info("开始处理 ZIP Entry, entry={}, size={}, object={}", entryName, entrySize, objectName);
                // 同名对象直接覆盖（PutObject / CompleteMultipartUpload 均会替换已有对象）
                try (InputStream zipInputStream = zipFile.getInputStream(entry); InputStream inputStream = new BufferedInputStream(zipInputStream, INPUT_BUFFER_SIZE)) {
                    if (entrySize <= DIRECT_UPLOAD_MAX_SIZE) {
                        uploadDirect(bucketName, objectName, inputStream, entrySize);
                    } else {
                        uploadMultipart(bucketName, objectName, inputStream, entrySize);
                    }
                }
                uploadedKeys.add(objectName);
                fileCount++;
                totalBytes += entrySize;
                log.info("ZIP Entry 处理完成, entry={}, size={}, fileCount={}", entryName, entrySize, fileCount);
            }
        } finally {
            if (deleteFlag) {
                FileUtils.deleteQuietly(zipPath.toFile());
                log.debug("删除 ZIP 文件, zipPath={}", zipPath);
            }
        }
        log.info("ZIP 解压上传完成, zipPath={}, fileCount={}, totalBytes={}", zipPath, fileCount, totalBytes);
        return new ExtractResult(fileCount, totalBytes, uploadedKeys);
    }

    /**
     * 尝试多种编码打开ZipFile，解决 CEN header(bad entry name) 编码异常
     */
    private ZipFile openZipWithEncodingTry(Path zipPath) throws IOException {
        Exception lastException = null;
        for (Charset charset : ZIP_CHARSET_TRY_LIST) {
            try {
                ZipFile zipFile = new ZipFile(zipPath.toFile(), charset);
                log.info("ZIP文件打开成功，使用编码：{}，zipPath={}", charset.name(), zipPath);
                return zipFile;
            } catch (ZipException e) {
                lastException = e;
                log.warn("尝试以编码[{}]打开ZIP失败，继续下一个编码尝试, zipPath={}, msg={}", charset.name(), zipPath, e.getMessage());
            }
        }
        // 全部编码尝试失败，抛出原始异常
        throw new IOException("ZIP文件无法打开，已尝试编码" + ZIP_CHARSET_TRY_LIST + ", path:" + zipPath, lastException);
    }

    /**
     * ZIP 解压上传结果
     */
    public static class ExtractResult {
        /** 文件数量（不含目录） */
        private final long fileCount;
        /** 文件总大小（字节） */
        private final long totalBytes;
        /** 本次成功上传的对象 key */
        private final List<String> uploadedObjectKeys;

        public ExtractResult(long fileCount, long totalBytes) {
            this(fileCount, totalBytes, new ArrayList<>());
        }

        public ExtractResult(long fileCount, long totalBytes, List<String> uploadedObjectKeys) {
            this.fileCount = fileCount;
            this.totalBytes = totalBytes;
            this.uploadedObjectKeys = uploadedObjectKeys != null ? uploadedObjectKeys : new ArrayList<>();
        }

        public long getFileCount() {
            return fileCount;
        }

        public long getTotalBytes() {
            return totalBytes;
        }

        public List<String> getUploadedObjectKeys() {
            return uploadedObjectKeys;
        }
    }

    /**
     * 小文件直接上传。
     */
    private void uploadDirect(String bucketName, String objectName, InputStream inputStream, long contentLength) {
        try {
            ObjectMetadata metadata = new ObjectMetadata();
            metadata.setContentLength(contentLength);
            PutObjectRequest request = new PutObjectRequest(bucketName, objectName, inputStream, metadata);
            amazonS3Client.putObject(request);
            log.debug("PutObject 上传完成, bucket={}, object={}, size={}", bucketName, objectName,
                    contentLength);
        } catch (Exception e) {
            log.error("PutObject 上传异常, bucket={}, object={}, size={}", bucketName, objectName, contentLength, e);
        }
    }

    /**
     * 大文件 Multipart Upload。
     *
     * <p>
     * 核心特点：
     *
     * <pre>
     * ZIP Entry
     *     ↓
     * InputStream
     *     ↓
     * 64MB byte[]
     *     ↓
     * Part
     *     ↓
     * MinIO
     * </pre>
     * <p>
     * 不会随着 Entry 大小增加内存。
     */
    private void uploadMultipart(String bucketName, String objectName, InputStream inputStream, long contentLength) throws IOException {
        InitiateMultipartUploadRequest initiateRequest = new InitiateMultipartUploadRequest(bucketName, objectName);
        InitiateMultipartUploadResult initiateResult = amazonS3Client.initiateMultipartUpload(initiateRequest);
        String uploadId = initiateResult.getUploadId();
        List<PartETag> partETags = new ArrayList<>();
        /*
         * 整个 Multipart Upload 生命周期只创建一个 byte[]。
         *
         * 例如：
         *
         * 64MB
         *
         * 第一次使用：
         * Part 1
         *
         * 上传完成后继续复用：
         * Part 2
         *
         * ...
         */
        byte[] buffer = new byte[PART_SIZE];
        int partNumber = 1;
        long remaining = contentLength;
        try {
            while (remaining > 0) {
                int currentPartSize = (int) Math.min(PART_SIZE, remaining);
                int actualSize = readPart(inputStream, buffer, currentPartSize);
                if (actualSize <= 0) {
                    throw new IOException("ZIP Entry 数据提前结束, " + "object=" + objectName + ", expectedRemaining=" + remaining);
                }
                /*
                 * 注意：
                 *
                 * 这里不能直接把整个 64MB buffer
                 * 都上传。
                 *
                 * 最后一个 Part 很可能小于 64MB。
                 */
                ByteArrayInputStream partInputStream = new ByteArrayInputStream(buffer, 0, actualSize);
                UploadPartRequest uploadPartRequest = new UploadPartRequest().withBucketName(bucketName).withKey(objectName).withUploadId(uploadId).withPartNumber(partNumber).withInputStream(partInputStream).withPartSize(actualSize);
                UploadPartResult uploadPartResult = amazonS3Client.uploadPart(uploadPartRequest);
                partETags.add(uploadPartResult.getPartETag());
                remaining -= actualSize;
                log.info("Multipart Part 上传完成, " + "object={}, uploadId={}, partNumber={}, " + "partSize={}, remaining={}", objectName, uploadId, partNumber, actualSize, remaining);
                partNumber++;
            }
            CompleteMultipartUploadRequest completeRequest = new CompleteMultipartUploadRequest(bucketName, objectName, uploadId, partETags);
            amazonS3Client.completeMultipartUpload(completeRequest);
            log.info("Multipart Upload 完成, " + "object={}, uploadId={}, partCount={}", objectName, uploadId, partETags.size());
        } catch (Exception e) {
            log.error("Multipart Upload 失败, " + "bucket={}, object={}, uploadId={}", bucketName, objectName, uploadId, e);
            /*
             * 非常重要：
             *
             * Multipart Upload 失败后，
             * 必须 Abort。
             *
             * 否则 MinIO 中可能残留未完成的 Multipart Upload。
             */
            abortMultipartUpload(bucketName, objectName, uploadId);
            if (e instanceof IOException) {
                throw (IOException) e;
            }
            throw new IOException("Multipart Upload 失败: " + objectName, e);
        }
    }

    /**
     * 从 ZIP Entry 中读取一个 Part。
     *
     * <p>
     * 不保证一次 read() 就读取完，
     * 所以必须循环读取。
     */
    private int readPart(InputStream inputStream, byte[] buffer, int expectedSize) throws IOException {
        int totalRead = 0;
        while (totalRead < expectedSize) {
            int read = inputStream.read(buffer, totalRead, expectedSize - totalRead);
            if (read == -1) {
                break;
            }
            if (read == 0) {
                continue;
            }
            totalRead += read;
        }
        return totalRead;
    }

    /**
     * Abort Multipart Upload。
     */
    private void abortMultipartUpload(String bucketName, String objectName, String uploadId) {
        try {
            amazonS3Client.abortMultipartUpload(new AbortMultipartUploadRequest(bucketName, objectName, uploadId));
            log.info("Multipart Upload 已 Abort, " + "bucket={}, object={}, uploadId={}", bucketName, objectName, uploadId);
        } catch (Exception e) {
            /*
             * Abort 失败不能覆盖原始异常。
             * 这里只记录日志。
             */
            log.error("Abort Multipart Upload 失败, " + "bucket={}, object={}, uploadId={}", bucketName, objectName, uploadId, e);
        }
    }

    /**
     * ZIP 文件基础检查。
     */
    private void checkZipFile(Path zipPath) throws IOException {
        if (zipPath == null) {
            throw new IllegalArgumentException("zipPath 不能为空");
        }
        if (!Files.exists(zipPath)) {
            throw new IOException("ZIP 文件不存在: " + zipPath);
        }
        if (!Files.isRegularFile(zipPath)) {
            throw new IOException("ZIP 路径不是普通文件: " + zipPath);
        }
        if (!Files.isReadable(zipPath)) {
            throw new IOException("ZIP 文件不可读取: " + zipPath);
        }
        if (Files.size(zipPath) <= 0) {
            throw new IOException("ZIP 文件为空: " + zipPath);
        }
    }

    /**
     * ZIP Entry 安全检查。
     * <p>
     * 虽然这里最终上传到 MinIO，
     * 而不是直接解压到本地目录，
     * 仍然建议保留这层检查。
     */
    private void validateEntryName(String entryName) throws IOException {
        if (entryName == null || entryName.isBlank()) {
            throw new IOException("ZIP Entry 文件名为空");
        }
        String normalized = entryName.replace('\\', '/');
        // Unix 绝对路径
        if (normalized.startsWith("/")) {
            throw new IOException("非法 ZIP Entry：绝对路径, " + entryName);
        }
        // Windows 绝对路径
        if (normalized.matches("^[a-zA-Z]:/.*")) {
            throw new IOException("非法 ZIP Entry：Windows 绝对路径, " + entryName);
        }
        String[] parts = normalized.split("/");
        for (String part : parts) {
            if ("..".equals(part)) {
                throw new IOException("非法 ZIP Entry：路径穿越, " + entryName);
            }
        }
    }
}
