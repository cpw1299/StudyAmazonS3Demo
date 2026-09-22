package com.yhcx.framework.file.core.service;

import cn.hutool.core.io.FileUtil;
import cn.hutool.core.io.IORuntimeException;
import cn.hutool.core.io.IoUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.extra.spring.SpringUtil;
import com.amazonaws.HttpMethod;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.*;
import com.yhcx.framework.common.dto.WebdavDTO;
import com.yhcx.framework.common.pojo.PageResult;
import com.yhcx.framework.common.util.file.Tools;
import com.yhcx.framework.common.util.object.PageUtils;
import com.yhcx.framework.file.core.convert.S3Convert;
import com.yhcx.framework.file.core.dto.FileDTO;
import com.yhcx.framework.file.core.dto.FileDownloadResult;
import com.yhcx.framework.file.core.dto.FileRespDTO;
import com.yhcx.framework.file.core.dto.FolderRespDTO;
import com.yhcx.module.infra.framework.file.core.client.FileClient;
import com.yhcx.module.infra.api.file.dto.FilePresignedUrlRespDTO;
import com.yhcx.module.infra.framework.file.core.client.s3.S3FileClientConfig;
import com.yhcx.module.infra.framework.file.core.utils.FileTypeUtils;
import com.yhcx.module.infra.service.file.FileConfigService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipArchiveInputStream;
import org.apache.commons.lang3.StringUtils;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.util.CollectionUtils;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import javax.servlet.http.HttpServletResponse;
import javax.validation.constraints.NotBlank;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * FileStoragePlusUltraService
 *
 * @author liuhm
 * @version 1.0.0
 * @create 2025/11/12 15:36
 **/
@Slf4j
public class S3FileStoragePlusUltraServiceImpl implements FileStoragePlusUltraService {

    @Resource
    private FileConfigService fileConfigService;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    private static S3FileClientConfig s3FileClientConfig;

    private static AmazonS3Client s3FileClient;

    private static final Integer PAGE_SIZE = 10;

    public static final String DATASET_ROOT_PATH = "dataset";

    /**
     * 缓冲区大小（IO缓冲区推荐 8192 字节）
     */
    private static final int BUFFER_SIZE = 8192;
    // ===================== copyZipToDataset 专用常量 =====================
    private static final int MIN_UPLOAD_THREAD = 4;
    private static final int MAX_UPLOAD_THREAD = 32;
    private static final int QUEUE_MULTIPLE = 20;
    // 单文件内存上限1GB，超过直接熔断，防止OOM
    private static final long SINGLE_FILE_MEM_LIMIT = 1L * 1024 * 1024 * 1024;
    private static final long POOL_WAIT_MINUTES = 30;

    @PostConstruct
    public void init() {
        FileClient fileClient = fileConfigService.getMasterFileClient();
        s3FileClient = (AmazonS3Client) Objects.requireNonNull(fileClient).getClient();
        s3FileClientConfig = (S3FileClientConfig) fileClient.getFileConfig();
    }

    @Override
    @Cacheable(value = "s3#600s", key = "'list:'+#relativeDatasetRootPath+#path", unless = "#result == null || #result.size() == 0")
    public List<FileRespDTO> fileList(WebdavDTO webdavDTO, String relativeDatasetRootPath, @NotBlank String path) {
        String storePath = DATASET_ROOT_PATH + Tools.join(relativeDatasetRootPath, path);

        ObjectListing objectListing = s3FileClient.listObjects(new ListObjectsRequest()
                .withBucketName(s3FileClientConfig.getBucket())
                .withMaxKeys(Integer.MAX_VALUE)
                .withDelimiter("/").withPrefix(storePath + "/"));

        List<FileRespDTO> fileList = new ArrayList<>();
        S3Convert.listFolder(relativeDatasetRootPath, path, objectListing.getCommonPrefixes(), fileList);
        S3Convert.listFile(storePath, path, objectListing.getObjectSummaries(), fileList);
        return fileList;
    }

    @Override
    public List<FileRespDTO> fileDirectList(WebdavDTO webdavDTO, String relativeDatasetRootPath, @NotBlank String path) {
        String storePath = DATASET_ROOT_PATH + Tools.join(relativeDatasetRootPath, path);

        List<S3ObjectSummary> s3ObjectSummaries = new ArrayList<>();
        ListObjectsV2Request listObjectsRequest = new ListObjectsV2Request()
                .withBucketName(s3FileClientConfig.getBucket()).withPrefix(storePath + "/");
        ListObjectsV2Result result;
        do {
            result = s3FileClient.listObjectsV2(listObjectsRequest);
            s3ObjectSummaries.addAll(result.getObjectSummaries());
            String token = result.getNextContinuationToken();
            listObjectsRequest.setContinuationToken(token);
        } while (result.isTruncated());

        List<FileRespDTO> fileList = new ArrayList<>();
        S3Convert.listFile(storePath, path, s3ObjectSummaries, fileList);
        return fileList;
    }

    private List<FileRespDTO> fileDirectListToDataset(String relativeDatasetRootPath, @NotBlank String path) {
        String storePath = this.buildFinalObjectPath(Tools.join(relativeDatasetRootPath, path));
        List<S3ObjectSummary> s3ObjectSummaries = new ArrayList<>();
        ListObjectsV2Request listObjectsRequest = new ListObjectsV2Request()
                .withBucketName(s3FileClientConfig.getBucket()).withPrefix(Tools.removePrefix(storePath) + "/");
        ListObjectsV2Result result;
        do {
            result = s3FileClient.listObjectsV2(listObjectsRequest);
            s3ObjectSummaries.addAll(result.getObjectSummaries());
            String token = result.getNextContinuationToken();
            listObjectsRequest.setContinuationToken(token);
        } while (result.isTruncated());

        List<FileRespDTO> fileList = new ArrayList<>();
        S3Convert.listFile(storePath, path, s3ObjectSummaries, fileList);
        return fileList;
    }

    private List<FileRespDTO> fileMaasDirectListToDataset(String relativeDatasetRootPath, @NotBlank String path) {
        String storePath = Tools.join(relativeDatasetRootPath, path);
        List<S3ObjectSummary> s3ObjectSummaries = new ArrayList<>();
        ListObjectsV2Request listObjectsRequest = new ListObjectsV2Request()
                .withBucketName(s3FileClientConfig.getBucket()).withPrefix(Tools.removePrefix(storePath) + "/");
        ListObjectsV2Result result;
        do {
            result = s3FileClient.listObjectsV2(listObjectsRequest);
            s3ObjectSummaries.addAll(result.getObjectSummaries());
            String token = result.getNextContinuationToken();
            listObjectsRequest.setContinuationToken(token);
        } while (result.isTruncated());

        List<FileRespDTO> fileList = new ArrayList<>();
        S3Convert.listFile(storePath, path, s3ObjectSummaries, fileList);
        return fileList;
    }

    /**
     * 根据bucketName获取文件列表
     *
     * @param bucketName
     * @param relativeDatasetRootPath
     * @param path
     * @return
     */
    private List<FileRespDTO> getFileByBucketFilePath(String bucketName, String relativeDatasetRootPath, @NotBlank String path) {
        String storePath = Tools.join(relativeDatasetRootPath, path);
        List<S3ObjectSummary> s3ObjectSummaries = new ArrayList<>();
        ListObjectsV2Request listObjectsRequest = new ListObjectsV2Request()
                .withBucketName(bucketName).withPrefix(Tools.removePrefix(storePath) + "/");
        ListObjectsV2Result result;
        do {
            result = s3FileClient.listObjectsV2(listObjectsRequest);
            s3ObjectSummaries.addAll(result.getObjectSummaries());
            String token = result.getNextContinuationToken();
            listObjectsRequest.setContinuationToken(token);
        } while (result.isTruncated());

        List<FileRespDTO> fileList = new ArrayList<>();
        S3Convert.listFile(storePath, path, s3ObjectSummaries, fileList);
        return fileList;
    }


    /**
     * 统计指定路径下文件的数量（仅size非空且非0的文件）和总大小
     *
     * @param webdavDTO               WebDAV相关配置DTO
     * @param relativeDatasetRootPath 数据集根路径相对路径
     * @param path                    待统计的目标路径（非空）
     * @return FileDTO 包含文件数量和总大小的DTO
     */
    @Override
    public FileDTO fileCountSize(WebdavDTO webdavDTO,
                                 String relativeDatasetRootPath,
                                 @NotBlank String path) {
        // 1. 获取文件列表，防御性处理空指针
        List<FileRespDTO> fileRespDtos = fileDirectList(webdavDTO, relativeDatasetRootPath, path);
        if (CollectionUtils.isEmpty(fileRespDtos)) {
            return new FileDTO(0L, 0L);
        }

        // 2. 流式统计：仅计算size非空且非0的文件数量和总大小
        AtomicLong totalFileCount = new AtomicLong(0L);
        AtomicLong totalFileSize = new AtomicLong(0L);

        fileRespDtos.stream()
                .map(FileRespDTO::getSize) // 提取文件大小
                .filter(Objects::nonNull)  // 过滤size为空的情况
                .filter(size -> size != 0L) // 过滤size为0的情况
                .forEach(size -> {
                    totalFileCount.incrementAndGet(); // 计数+1
                    totalFileSize.addAndGet(size);    // 累加大小
                });

        // 3. 返回统计结果
        return new FileDTO(totalFileCount.get(), totalFileSize.get());
    }

    @Override
    public PageResult<FileRespDTO> filePageList(WebdavDTO webdavDTO, String relativeDatasetRootPath,
                                                @NotBlank String path, Integer pageNo) {
        List<FileRespDTO> list = getSelf().fileList(webdavDTO, relativeDatasetRootPath, path);
        List<FileRespDTO> pageList = PageUtils.getPageLimit(list, pageNo, PAGE_SIZE);
        return new PageResult<>(pageList, (long) list.size());
    }

    @Override
    public List<FolderRespDTO> folderList(WebdavDTO webdavDTO, String relativeDatasetRootPath,
                                          @NotBlank String path, Integer level) {
        String storePath = Tools.join(DATASET_ROOT_PATH, relativeDatasetRootPath, path);

        List<S3ObjectSummary> s3ObjectSummaries = new ArrayList<>();
        ListObjectsV2Request listObjectsRequest = new ListObjectsV2Request()
                .withBucketName(s3FileClientConfig.getBucket())
                .withPrefix(storePath);
        ListObjectsV2Result result;
        do {
            result = s3FileClient.listObjectsV2(listObjectsRequest);
            s3ObjectSummaries.addAll(result.getObjectSummaries());
            String token = result.getNextContinuationToken();
            listObjectsRequest.setContinuationToken(token);
        } while (result.isTruncated());

        return S3Convert.listFolderRecursively(storePath, path, result.getObjectSummaries());
    }

    @Override
    public String upload(WebdavDTO webdavDTO, byte[] content, @NotBlank String path) {
        path = Tools.join(DATASET_ROOT_PATH, path);
        // 元数据，主要用于设置文件类型
        ObjectMetadata objectMetadata = new ObjectMetadata();
        // 计算默认的 path 名
        String type = FileTypeUtils.getMimeType(content);
        objectMetadata.setContentType(type);
        // 如果不设置，会有 “ No content length specified for stream data” 警告日志
        objectMetadata.setContentLength(content.length);
        // 执行上传
        s3FileClient.putObject(s3FileClientConfig.getBucket(),
                path, new ByteArrayInputStream(content), objectMetadata);

        // 拼接返回路径
        return "/" + path;
    }

    @Override
    public String uploadUrl(WebdavDTO webdavDTO, byte[] content, String path) {
        return s3FileClientConfig.getDomain() + upload(webdavDTO, content, path);
    }

    @Override
    public String uploadLargeFile(WebdavDTO webdavDTO, String uid, byte[] content, @NotBlank String path) {

        return s3FileClientConfig.getDomain() + upload(webdavDTO, content, path);
    }

    @Override
    public void delete(WebdavDTO webdavDTO, @NotBlank String path) {
        s3FileClient.deleteObject(s3FileClientConfig.getBucket(), Tools.join(DATASET_ROOT_PATH, path));
    }

    @Override
    public byte[] getContent(WebdavDTO webdavDTO, String path) {
        String pathFile = FileTypeUtils.ensurePathStartWithSlash(path);
        String pathFileRoot = FileTypeUtils.ensurePathStartWithSlash(DATASET_ROOT_PATH);
        String objPath = StringUtils.startsWith(pathFile, pathFileRoot) ? path : Tools.join(pathFileRoot, path);
        S3Object tempS3Object = s3FileClient.getObject(s3FileClientConfig.getBucket(), objPath);
        return IoUtil.readBytes(tempS3Object.getObjectContent());
    }

    @Override
    public byte[] getContentPackTargetFoldersRecursively(String repositoryPath, List<String> targetFolders) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ZipOutputStream zos = new ZipOutputStream(bos, StandardCharsets.UTF_8);
        boolean hasValidFile = false;
        try {
            // ========== 新增逻辑：空列表时打包根路径所有内容 ==========
            if (targetFolders == null || targetFolders.isEmpty()) {
                // 规范根路径（以/结尾），递归打包根路径repositoryPath下所有内容
                String rootFolder = "/";
                hasValidFile = packFolderAndAllChildren(zos, repositoryPath, rootFolder, true);
            } else {
                // ========== 原有逻辑：非空时打包指定目标文件夹 ==========
                for (String targetFolder : targetFolders) {
                    if (StrUtil.isBlank(targetFolder)) {
                        log.warn("跳过空的目标文件夹：repositoryPath={}", repositoryPath);
                        continue;
                    }
                    boolean folderHasFile = packFolderAndAllChildren(zos, repositoryPath, targetFolder, false);
                    hasValidFile = hasValidFile || folderHasFile;
                }
            }

            zos.finish();

            // 无有效文件则返回null（避免生成空压缩包）
            if (!hasValidFile) {
                log.error("所有目标文件夹均无有效文件：{}", targetFolders);
                return null;
            }

            return bos.toByteArray();

        } catch (IOException e) {
            log.error("目标文件夹递归打包异常：repositoryPath={}，targetFolders={}", repositoryPath, targetFolders, e);
            throw new IORuntimeException("目标文件夹打包失败", e);
        } finally {
            // 强制关闭流
            try {
                zos.close();
            } catch (IOException e) {
                log.error("关闭ZipOutputStream失败", e);
            }
            try {
                bos.close();
            } catch (IOException e) {
                log.error("关闭ByteArrayOutputStream失败", e);
            }
        }
    }


    /**
     * 递归打包文件到 ZIP（阿里云流式大文件规范实现）
     * 遍历指定目录下所有文件，递归写入 ZIP 输出流，保持原有目录结构
     * 全程流式读写，不加载文件到内存，支持 100GB+ 大文件
     *
     * @param zos            ZIP 输出流
     * @param repositoryPath 仓库根路径
     * @param currentFolder  当前操作文件夹
     * @param isRootPack     是否根目录打包（用于路径裁剪）
     * @return boolean 是否存在有效文件
     */
    private boolean packFolderAndAllChildren(ZipOutputStream zos, String repositoryPath,
                                             String currentFolder, boolean isRootPack) {
        // 标记是否存在有效文件（用于最终状态判断）
        boolean hasValidFile = false;

        try {
            // 1. 获取当前目录下文件列表（全量遍历，支持海量文件）
            List<FileRespDTO> fileList = fileDirectListToDataset(repositoryPath, currentFolder);
            if (CollectionUtils.isEmpty(fileList)) {
                log.info("[递归打包] 当前目录无文件，folder:{}", currentFolder);
                return false;
            }

            // 2. 遍历文件列表，逐个流式写入 ZIP
            for (FileRespDTO file : fileList) {
                // 过滤文件夹 & 过滤空路径文件
                if (Boolean.TRUE.equals(file.getFolder()) || StrUtil.isBlank(file.getOriginalPath())) {
                    continue;
                }

                // 文件原始路径
                String originalPath = file.getOriginalPath();
                log.info("[递归打包] 开始处理文件，folder:{}，原文件路径:{}", repositoryPath + currentFolder, originalPath);

                // 3. 获取文件输入流（S3 流式读取，不加载内存）
                try (InputStream rawStream = getInputStream(null, originalPath)) {
                    if (rawStream == null) {
                        log.warn("[递归打包] 文件流获取为空，文件路径:{}", originalPath);
                        continue;
                    }

                    // 包装回推流，预读1字节判断是否为空文件
                    PushbackInputStream inputStream = new PushbackInputStream(rawStream);
                    int firstByte = inputStream.read();
                    // -1 代表文件0字节，直接跳过
                    if (firstByte == -1) {
                        log.warn("[递归打包] 文件为空字节，跳过打包，文件路径:{}", originalPath);
                        continue;
                    }
                    // 将读取的首字节推回流，后续正常拷贝流
                    inputStream.unread(firstByte);

                    // 4. 构建 ZIP 内部文件路径（保持目录结构）
                    String entryName = buildZipEntryPath(originalPath, repositoryPath, isRootPack);
                    // 创建 ZIP 条目
                    zos.putNextEntry(new ZipEntry(entryName));

                    // 5. 流拷贝（阿里云规范：固定缓冲区 8KB，循环读写，永不 OOM）
                    byte[] buffer = new byte[BUFFER_SIZE];
                    int len;
                    while ((len = inputStream.read(buffer)) != -1) {
                        zos.write(buffer, 0, len);
                    }

                    // 6. 关闭当前 ZIP 条目，写入完成
                    zos.closeEntry();
                    hasValidFile = true;

                    // 成功日志
                    log.info("[递归打包] 单文件写入 ZIP 成功，原路径:{}，ZIP 内部路径:{}", originalPath, entryName);

                } catch (Exception e) {
                    // 单个文件异常不影响整体打包，只打印警告日志
                    log.warn("[递归打包] 单文件写入 ZIP 失败，文件路径:{}，异常信息:{}", originalPath, e.getMessage(), e);
                }
            }

        } catch (Exception e) {
            // 目录遍历异常，打印错误日志
            log.error("[递归打包] 执行异常，仓库路径:{}, 当前文件夹:{}", repositoryPath, currentFolder, e);
        }

        return hasValidFile;
    }

    /**
     * 构建 ZIP 内部文件路径（保持目录结构）
     */
    private String buildZipEntryPath(String itemPath, String repositoryPath, boolean isRootPack) {
        if (isRootPack) {
            String rootPath = buildFinalObjectPath(repositoryPath);
            String raw = StrUtil.replaceFirst(itemPath, rootPath, "");
            return StrUtil.strip(raw, "/");
        } else {
            return StrUtil.strip(itemPath, "/");
        }
    }

    @Override
    public InputStream getInputStream(WebdavDTO webdavDTO, String path) {
        String pathFile = FileTypeUtils.ensurePathStartWithSlash(path);
        String pathFileRoot = FileTypeUtils.ensurePathStartWithSlash(DATASET_ROOT_PATH);
        String objPath = StringUtils.startsWith(pathFile, pathFileRoot) ? path : Tools.join(pathFileRoot, path);
        S3Object tempS3Object = s3FileClient.getObject(s3FileClientConfig.getBucket(), objPath);
        return tempS3Object.getObjectContent();
    }

    @Override
    public byte[] folderFileDownload(WebdavDTO webdavDTO, String zipName,
                                     String relativeDatasetRootPath, String path) {
        List<java.util.concurrent.Future<FileDownloadResult>> futures = new ArrayList<>();
        // 线程池并发下载
        ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 2);

        // 获取所有文件
        List<FileRespDTO> allFiles = fileDirectList(webdavDTO, relativeDatasetRootPath, path);
        if (allFiles.isEmpty()) {
            return null;
        }

        for (FileRespDTO file : allFiles) {
            if (file.getFolder()) {
                continue;
            }
            futures.add(executor.submit(() -> {
                try {
                    byte[] content = getContent(webdavDTO, file.getOriginalPath());
                    return new FileDownloadResult(file, content, null);
                } catch (Exception e) {
                    log.error("下载文件失败: {}，error: {}", file.getOriginalPath(), e.getMessage());
                    return new FileDownloadResult(file, null, e);
                }
            }));
        }

        // 顺序写入zip
        String zipPath = Tools.join(System.getProperty("java.io.tmpdir"), zipName);
        try (FileOutputStream fos = new FileOutputStream(zipPath); ZipOutputStream zos = new ZipOutputStream(fos)) {
            byte[] buffer = new byte[8192];
            for (int i = 0; i < futures.size(); i++) {
                FileDownloadResult result;
                try {
                    result = futures.get(i).get();
                } catch (Exception e) {
                    log.error("获取下载结果失败: {}", e.getMessage());
                    continue;
                }
                if (result.getContent() == null) continue;

                zos.putNextEntry(new ZipEntry(StrUtil.subAfter(result.getFile().getPath(), path, true)));
                try (InputStream is = new ByteArrayInputStream(result.getContent())) {
                    int len;
                    while ((len = is.read(buffer)) > 0) {
                        zos.write(buffer, 0, len);
                    }
                } catch (Exception e) {
                    log.error("写入zip文件失败: {}", e.getMessage());
                    continue;
                }
                zos.closeEntry();
            }
        } catch (Exception e) {
            log.error("压缩文件失败: {}", e.getMessage());
            return null;
        } finally {
            executor.shutdown();
        }
        return FileUtil.readBytes(zipPath);
    }

    @Override
    public void folderFileDownload(WebdavDTO webdavDTO, String zipName, String relativeDatasetRootPath, String path,
                                   HttpServletResponse response) {
        List<Future<FileDownloadResult>> futures = new ArrayList<>();
        // 线程池并发下载
        ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 2);

        // 获取所有文件
        List<FileRespDTO> allFiles = fileDirectList(webdavDTO, relativeDatasetRootPath, path);
        if (allFiles.isEmpty()) {
            throw new RuntimeException("数据集文件列表为空");
        }
        response.setContentType("application/zip");
        String zipFileName = zipName + ".zip";
        response.setHeader("Content-Disposition", "attachment; filename=\"" + zipFileName + "\"");

        for (FileRespDTO file : allFiles) {
            if (file.getFolder()) {
                continue;
            }
            futures.add(executor.submit(() -> {
                try {
                    byte[] content = getContent(webdavDTO, file.getOriginalPath());
                    return new FileDownloadResult(file, content, null);
                } catch (Exception e) {
                    log.error("下载文件失败: {}，error: {}", file.getOriginalPath(), e.getMessage());
                    return new FileDownloadResult(file, null, e);
                }
            }));
        }

        // 顺序写入zip
        try (ZipOutputStream zos = new ZipOutputStream(response.getOutputStream())) {
            byte[] buffer = new byte[8192];
            for (int i = 0; i < futures.size(); i++) {
                FileDownloadResult result;
                try {
                    result = futures.get(i).get();
                } catch (Exception e) {
                    log.error("获取下载结果失败: {}", e.getMessage());
                    continue;
                }
                if (result.getContent() == null) continue;

                zos.putNextEntry(new ZipEntry(StrUtil.subAfter(result.getFile().getPath(), path, true)));
                try (InputStream is = new ByteArrayInputStream(result.getContent())) {
                    int len;
                    while ((len = is.read(buffer)) > 0) {
                        zos.write(buffer, 0, len);
                    }
                } catch (Exception e) {
                    log.error("写入zip文件失败: {}", e.getMessage());
                    continue;
                }
                zos.closeEntry();
            }
            zos.finish();
        } catch (Exception e) {
            log.error("压缩文件失败: {}", e.getMessage());
            throw new RuntimeException("压缩文件失败");
        } finally {
            executor.shutdown();
        }
    }

    @Override
    public void deleteDavResourceFromCache(WebdavDTO webdavDTO, String relativeDatasetRootPath, String path) {
        stringRedisTemplate.delete("s3:list:" + relativeDatasetRootPath + path);
    }

    @Override
    public void createDirectory(WebdavDTO webdavDTO, String relativeDatasetRootPath, String path) {
        String storePath = Tools.join(DATASET_ROOT_PATH, relativeDatasetRootPath, path, ".tmp");
        // 元数据，主要用于设置文件类型
        ObjectMetadata objectMetadata = new ObjectMetadata();
        objectMetadata.setContentType("tmp");
        // 如果不设置，会有 “ No content length specified for stream data” 警告日志
        objectMetadata.setContentLength(0L);
        // 执行上传
        s3FileClient.putObject(s3FileClientConfig.getBucket(),
                storePath, new ByteArrayInputStream(new byte[0]), objectMetadata);
    }

    @Override
    public void copyDirectory(WebdavDTO webdavDTO, String sourcePath, String targetPath) {
        // 获取源文件列表
        List<FileRespDTO> fileList = fileDirectList(webdavDTO, sourcePath, "");
        for (FileRespDTO fileRespDTO : fileList) {
            // DATASET_ROOT_PATH + 替换路径
            String targetFilePath = Tools.join(fileRespDTO.getOriginalPath().replace(sourcePath, targetPath));
            // 复制文件
            copyFile(fileRespDTO.getOriginalPath(), targetFilePath);
        }
    }

    @Override
    public void copyDirectoryToDataset(WebdavDTO webdavDTO, String sourcePath, String targetPath) {
        // 获取源文件列表
        List<FileRespDTO> fileList = fileDirectListToDataset(sourcePath, "");
        log.info(">>> 文件列表: filePath:{},{}", sourcePath, fileList);
        for (FileRespDTO fileRespDTO : fileList) {
            // DATASET_ROOT_PATH + 替换路径
            String targetFilePath = Tools.join(DATASET_ROOT_PATH, fileRespDTO.getOriginalPath().replace(sourcePath, targetPath));
            // 复制文件
            copyFile(fileRespDTO.getOriginalPath(), targetFilePath);
        }
    }

    @Override
    public void copyMaasDirectoryToDataset(WebdavDTO webdavDTO, String sourcePath, String targetPath) {
        // 获取源文件列表
        List<FileRespDTO> fileList = fileMaasDirectListToDataset(sourcePath, "");
        log.info(">>> 文件列表: filePath:{},{}", sourcePath, fileList);
        for (FileRespDTO fileRespDTO : fileList) {
            // DATASET_ROOT_PATH + 替换路径
            String targetFilePath = Tools.join(DATASET_ROOT_PATH, fileRespDTO.getOriginalPath().replace(sourcePath, targetPath));
            // 复制文件
            copyFile(fileRespDTO.getOriginalPath(), targetFilePath);
        }
    }

    @Override
    public void copyFolderBucketFilePathToBucketTargetPath(WebdavDTO webdavDTO, String sourceBucketName, String sourceFolderPath, String targetBucketName, String targetFolderPath) {
        // 获取源文件列表
        List<FileRespDTO> fileList = getFileByBucketFilePath(sourceBucketName, sourceFolderPath, "");
        log.info(">>> 文件列表: filePath:{},{}", sourceFolderPath, fileList);
        for (FileRespDTO fileRespDTO : fileList) {
            String targetFilePath = Tools.join(fileRespDTO.getOriginalPath().replace(sourceFolderPath, targetFolderPath));
            // 复制文件
            copyFileBucketFilePathToBucketTargetPath(webdavDTO, sourceBucketName, fileRespDTO.getOriginalPath(), targetBucketName, targetFilePath);
        }
    }

    @Override
    public void copyFile(String sourceFilePath, String targetFilePath) {
        // TODO 待实现
        CopyObjectRequest copyObjectRequest = new CopyObjectRequest(
                s3FileClientConfig.getBucket(), sourceFilePath,
                s3FileClientConfig.getBucket(), targetFilePath);
        s3FileClient.copyObject(copyObjectRequest);
    }

    @Override
    public void copyFileBucketFilePathToBucketTargetPath(WebdavDTO webdavDTO, String sourceBucketName, String sourceFilePath, String targetBucketName, String targetFilePath) {
        CopyObjectRequest copyObjectRequest = new CopyObjectRequest(
                sourceBucketName, sourceFilePath,
                targetBucketName, targetFilePath);
        s3FileClient.copyObject(copyObjectRequest);
    }

    @Override
    public FilePresignedUrlRespDTO getFilePresignedUrl(WebdavDTO webdavDTO, String relativeDatasetRootPath, String path) {
        String filePath = Tools.join(DATASET_ROOT_PATH, relativeDatasetRootPath, path);
        // 设定过期时间为 10 分钟。取值范围：1 秒 ~ 7 天
        Date expiration = new Date(System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(10));
        // 生成上传 URL
        String uploadUrl = String.valueOf(s3FileClient.generatePresignedUrl(s3FileClientConfig.getBucket(), filePath, expiration, HttpMethod.GET));
        return new FilePresignedUrlRespDTO(uploadUrl, s3FileClientConfig.getDomain() + filePath, null);
    }

    /**
     * 流式压缩文件夹并直接写入输出流（支持超大文件，阿里云规范推荐）
     * 不会加载整个文件到内存
     * 【超大文件专用】流式压缩，直接写入输出流（PIPE 模式，无内存占用）
     * 支持 100GB+ 文件，永不 OOM
     *
     * @param repositoryPath 仓库根路径
     * @param targetFolders  待压缩的目标文件夹列表
     * @param outputStream   响应输出流
     */
    @Override
    public void getTargetFoldersStream(String repositoryPath, List<String> targetFolders, OutputStream outputStream) {
        // 构建标准化S3对象根路径
        String rootRepositoryPath = buildFinalObjectPath(repositoryPath);
        log.info("[超大文件流式压缩] 开始执行，标准化根目录:{}", rootRepositoryPath);

        ZipOutputStream zos = null;
        try {
            // 手动创建，不使用 try-with-resources（避免提前关闭）
            zos = new ZipOutputStream(outputStream, StandardCharsets.UTF_8);
            boolean hasValidFile = false;

            // 目标文件夹为空，默认压缩根目录
            if (CollectionUtils.isEmpty(targetFolders)) {
                hasValidFile = packFolderAndAllChildren(zos, rootRepositoryPath, "/", Boolean.TRUE);
                log.info("[超大文件流式压缩] 目标文件夹为空，自动压缩根目录:{}", rootRepositoryPath);
            } else {
                // 遍历压缩指定文件夹列表
                for (String folder : targetFolders) {
                    if (StrUtil.isBlank(folder)) {
                        log.warn("[超大文件流式压缩] 跳过空文件夹路径");
                        continue;
                    }
                    boolean folderCompressResult = packFolderAndAllChildren(zos, rootRepositoryPath, folder, Boolean.FALSE);
                    hasValidFile |= folderCompressResult;
                    log.info("[超大文件流式压缩] 单文件夹压缩完成，文件夹路径:{}, 压缩结果:{}", folder, folderCompressResult);
                }
            }

            // 完成 ZIP 写入（关键：必须执行 finish）
            zos.finish();
            log.info("[超大文件流式压缩] 全部执行完成，是否存在有效文件:{}", hasValidFile);
        } catch (Exception e) {
            log.error("[超大文件流式压缩] 执行异常，仓库路径:{}, 目标文件夹:{}", repositoryPath, targetFolders, e);
            throw new RuntimeException("流式压缩文件夹失败，请检查文件服务或网络状态");
        } finally {
            // 安全关闭，只关闭一次，防止重复关闭导致 Deflater closed
            if (zos != null) {
                try {
                    zos.close();
                } catch (Exception ignored) {
                    // 忽略关闭异常
                }
            }
        }
    }

    public S3FileStoragePlusUltraServiceImpl getSelf() {
        return SpringUtil.getBean(getClass());
    }

    /**
     * 构建最终 S3 对象路径（自动补全 dataset 前缀）
     */
    private String buildFinalObjectPath(String path) {
        String rootPrefix = FileTypeUtils.ensurePathStartWithSlash(DATASET_ROOT_PATH);
        String cleanPath = FileTypeUtils.ensurePathStartWithSlash(path);
        return StringUtils.startsWith(cleanPath, rootPrefix) ? path : Tools.join(rootPrefix, path);
    }


    /**
     * 字节数组方式上传至S3
     */
    private void uploadByteDirectByBucketName(String sourceBucketName,String objPath, String fileName, byte[] fileBytes) throws IOException {
        ObjectMetadata meta = new ObjectMetadata();
        String contentType = FileTypeUtils.getContentType(fileName);
        meta.setContentType(contentType);
        meta.setContentLength(fileBytes.length);

        s3FileClient.putObject(
                sourceBucketName,
                objPath,
                new ByteArrayInputStream(fileBytes),
                meta
        );
    }

    /**
     * 字节数组方式上传至S3
     */
    private void uploadByteDirect(String objPath, String fileName, byte[] fileBytes) throws IOException {
        ObjectMetadata meta = new ObjectMetadata();
        String contentType = FileTypeUtils.getContentType(fileName);
        meta.setContentType(contentType);
        meta.setContentLength(fileBytes.length);

        s3FileClient.putObject(
                s3FileClientConfig.getBucket(),
                objPath,
                new ByteArrayInputStream(fileBytes),
                meta
        );
    }


    // ===================== copyZipToDataset 内部实体 =====================
    private static class UploadMetaTask {
        private final String objPath;
        private final String fileName;
        private final String sourceZipUrl;
        private final String entryRawName;
        private final long entrySize;

        public UploadMetaTask(String objPath, String fileName, String sourceZipUrl, String entryRawName, long entrySize) {
            this.objPath = objPath;
            this.fileName = fileName;
            this.sourceZipUrl = sourceZipUrl;
            this.entryRawName = entryRawName;
            this.entrySize = entrySize;
        }
    }

    /**
     * 从【非dataset路径】拷贝ZIP压缩包至dataset目录并完整流式解压
     * 优化：多线程并发上传、自适应有界线程池、防OOM、无未实现接口依赖
     * 规则：仅解压当前zip内全部层级子文件夹/文件，包内zip不二次解压，仅保存为普通文件
     */
    @Override
    public void copyZipToDataset(WebdavDTO webdavDTO, String sourceZipUrl, String targetPath) {
        // 自定义并发线程（此处替换为配置中心读取）
        Integer customUploadThread = null;
        int corePoolSize;
        if (customUploadThread != null && customUploadThread > 0) {
            corePoolSize = customUploadThread;
        } else {
            corePoolSize = Runtime.getRuntime().availableProcessors() * 2;
        }
        corePoolSize = Math.max(MIN_UPLOAD_THREAD, Math.min(corePoolSize, MAX_UPLOAD_THREAD));
        int queueCapacity = corePoolSize * QUEUE_MULTIPLE;

        // 有界线程池，队列满主线程执行限流
        BlockingQueue<Runnable> workQueue = new ArrayBlockingQueue<>(queueCapacity);
        ExecutorService executor = new ThreadPoolExecutor(
                corePoolSize,
                corePoolSize,
                0L, TimeUnit.MILLISECONDS,
                workQueue,
                new ThreadPoolExecutor.CallerRunsPolicy()
        );

        String cleanTargetRelPath = FileTypeUtils.cleanSafePath(targetPath);
        String targetBasePrefix = buildFinalObjectPath(cleanTargetRelPath);
        List<Future<?>> futureList = new ArrayList<>();

        try (
                InputStream sourceZipInputStream = getRawInputStream(webdavDTO, sourceZipUrl);
                ZipArchiveInputStream zipArchiveIn = new ZipArchiveInputStream(sourceZipInputStream, "GBK", true)
        ) {
            ZipArchiveEntry entry;
            while ((entry = zipArchiveIn.getNextZipEntry()) != null) {
                String entryRawName = entry.getName();
                String safeEntryName = FileTypeUtils.cleanSafePath(entryRawName);

                // 路径穿越防护
                if (safeEntryName.contains("..")) {
                    log.error("[copyZipToDataset] 压缩包存在路径穿越风险，源zip:{}, 非法条目:{}", sourceZipUrl, entryRawName);
                    throw new RuntimeException("压缩包包含非法逃逸路径：" + entryRawName);
                }
                safeEntryName = FileTypeUtils.trimSlash(safeEntryName);

                if (entry.isDirectory()) {
                    log.debug("[copyZipToDataset] 跳过目录标记 | dir={}", entryRawName);
                    continue;
                }

                long entrySize = entry.getSize();
                // 超大文件熔断
                if (entrySize > SINGLE_FILE_MEM_LIMIT) {
                    log.error("[copyZipToDataset] 压缩包内文件超过内存阈值，禁止处理，zip:{}, entry:{}, size:{}",
                            sourceZipUrl, entryRawName, entrySize);
                    throw new RuntimeException("文件单文件大小超过内存限制" + SINGLE_FILE_MEM_LIMIT / 1024 / 1024 + "MB，无法加载");
                }

                String fullSaveObjPath = FileTypeUtils.join(targetBasePrefix, safeEntryName);
                String fileName = FileTypeUtils.getFileName(safeEntryName);
                UploadMetaTask metaTask = new UploadMetaTask(fullSaveObjPath, fileName, sourceZipUrl, entryRawName, entrySize);

                Future<?> future = executor.submit(() -> {
                    try {
                        byte[] fileBytes = readSingleZipEntryBytes(webdavDTO, metaTask);
                        uploadByteDirect(metaTask.objPath, metaTask.fileName, fileBytes);
                        log.info("[copyZipToDataset] 上传成功，存储路径：{}", metaTask.objPath);
                    } catch (Exception e) {
                        log.error("[copyZipToDataset] 文件上传失败，源Zip:{}, 条目:{}, 目标路径:{}",
                                sourceZipUrl, metaTask.entryRawName, metaTask.objPath, e);
                        throw new CompletionException(e);
                    }
                });
                futureList.add(future);
            }

            log.info("[copyZipToDataset] 压缩包元信息解析完成，待上传文件总数：{}，并发线程数：{}，队列容量：{}",
                    futureList.size(), corePoolSize, queueCapacity);

            // 等待全部任务，任意失败抛出异常
            for (Future<?> future : futureList) {
                try {
                    future.get();
                } catch (ExecutionException e) {
                    throw new RuntimeException("部分文件上传失败", e.getCause());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("上传任务等待被中断", e);
                }
            }
        } catch (Exception e) {
            log.error("[copyZipToDataset] ZIP读取条目异常，源zip:{}, targetPath:{}", sourceZipUrl, targetPath, e);
            throw new RuntimeException("ZIP解压读取失败：" + e.getMessage(), e);
        } finally {
            executor.shutdown();
            try {
                if (!executor.awaitTermination(POOL_WAIT_MINUTES, TimeUnit.MINUTES)) {
                    List<Runnable> pendingTasks = executor.shutdownNow();
                    log.warn("[copyZipToDataset] 上传任务超时未完成，强制销毁线程池，剩余待执行任务数：{}", pendingTasks.size());
                }
            } catch (InterruptedException e) {
                executor.shutdownNow();
                Thread.currentThread().interrupt();
                log.error("[copyZipToDataset] 线程池等待终止被中断", e);
            }
        }
        log.info("[copyZipToDataset] 当前压缩包全部层级文件解压上传完成，源zip：{}", sourceZipUrl);
    }

    @Override
    public void copyZipBucketFilePathToBucketTargetPath(WebdavDTO webdavDTO, String sourceBucketName, String sourceZipUrl, String targetBucketName, String targetFolderPath) {
// 自定义并发线程（此处替换为配置中心读取）
        Integer customUploadThread = null;
        int corePoolSize;
        if (customUploadThread != null && customUploadThread > 0) {
            corePoolSize = customUploadThread;
        } else {
            corePoolSize = Runtime.getRuntime().availableProcessors() * 2;
        }
        corePoolSize = Math.max(MIN_UPLOAD_THREAD, Math.min(corePoolSize, MAX_UPLOAD_THREAD));
        int queueCapacity = corePoolSize * QUEUE_MULTIPLE;

        // 有界线程池，队列满主线程执行限流
        BlockingQueue<Runnable> workQueue = new ArrayBlockingQueue<>(queueCapacity);
        ExecutorService executor = new ThreadPoolExecutor(
                corePoolSize,
                corePoolSize,
                0L, TimeUnit.MILLISECONDS,
                workQueue,
                new ThreadPoolExecutor.CallerRunsPolicy()
        );

        String cleanTargetRelPath = FileTypeUtils.cleanSafePath(targetFolderPath);
        String targetBasePrefix = buildFinalObjectPath(cleanTargetRelPath);
        List<Future<?>> futureList = new ArrayList<>();

        try (
                InputStream sourceZipInputStream = getRawInputStreamByBucketName(webdavDTO, sourceBucketName,sourceZipUrl);
                ZipArchiveInputStream zipArchiveIn = new ZipArchiveInputStream(sourceZipInputStream, "GBK", true)
        ) {
            ZipArchiveEntry entry;
            while ((entry = zipArchiveIn.getNextZipEntry()) != null) {
                String entryRawName = entry.getName();
                String safeEntryName = FileTypeUtils.cleanSafePath(entryRawName);

                // 路径穿越防护
                if (safeEntryName.contains("..")) {
                    log.error("[copyZipToDataset] 压缩包存在路径穿越风险，源zip:{}, 非法条目:{}", sourceZipUrl, entryRawName);
                    throw new RuntimeException("压缩包包含非法逃逸路径：" + entryRawName);
                }
                safeEntryName = FileTypeUtils.trimSlash(safeEntryName);

                if (entry.isDirectory()) {
                    log.debug("[copyZipToDataset] 跳过目录标记 | dir={}", entryRawName);
                    continue;
                }

                long entrySize = entry.getSize();
                // 超大文件熔断
                if (entrySize > SINGLE_FILE_MEM_LIMIT) {
                    log.error("[copyZipToDataset] 压缩包内文件超过内存阈值，禁止处理，zip:{}, entry:{}, size:{}",
                            sourceZipUrl, entryRawName, entrySize);
                    throw new RuntimeException("文件单文件大小超过内存限制" + SINGLE_FILE_MEM_LIMIT / 1024 / 1024 + "MB，无法加载");
                }

                String fullSaveObjPath = FileTypeUtils.join(targetBasePrefix, safeEntryName);
                String fileName = FileTypeUtils.getFileName(safeEntryName);
                UploadMetaTask metaTask = new UploadMetaTask(fullSaveObjPath, fileName, sourceZipUrl, entryRawName, entrySize);

                Future<?> future = executor.submit(() -> {
                    try {
                        byte[] fileBytes = readSingleZipEntryBytes(webdavDTO, metaTask);
                        uploadByteDirectByBucketName(targetBucketName,metaTask.objPath, metaTask.fileName, fileBytes);
                        log.info("[copyZipToDataset] 上传成功，存储路径：{}", metaTask.objPath);
                    } catch (Exception e) {
                        log.error("[copyZipToDataset] 文件上传失败，源Zip:{}, 条目:{}, 目标路径:{}",
                                sourceZipUrl, metaTask.entryRawName, metaTask.objPath, e);
                        throw new CompletionException(e);
                    }
                });
                futureList.add(future);
            }

            log.info("[copyZipToDataset] 压缩包元信息解析完成，待上传文件总数：{}，并发线程数：{}，队列容量：{}",
                    futureList.size(), corePoolSize, queueCapacity);

            // 等待全部任务，任意失败抛出异常
            for (Future<?> future : futureList) {
                try {
                    future.get();
                } catch (ExecutionException e) {
                    throw new RuntimeException("部分文件上传失败", e.getCause());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("上传任务等待被中断", e);
                }
            }
        } catch (Exception e) {
            log.error("[copyZipToDataset] ZIP读取条目异常，源zip:{}, targetPath:{}", sourceZipUrl, targetFolderPath, e);
            throw new RuntimeException("ZIP解压读取失败：" + e.getMessage(), e);
        } finally {
            executor.shutdown();
            try {
                if (!executor.awaitTermination(POOL_WAIT_MINUTES, TimeUnit.MINUTES)) {
                    List<Runnable> pendingTasks = executor.shutdownNow();
                    log.warn("[copyZipToDataset] 上传任务超时未完成，强制销毁线程池，剩余待执行任务数：{}", pendingTasks.size());
                }
            } catch (InterruptedException e) {
                executor.shutdownNow();
                Thread.currentThread().interrupt();
                log.error("[copyZipToDataset] 线程池等待终止被中断", e);
            }
        }
        log.info("[copyZipToDataset] 当前压缩包全部层级文件解压上传完成，源zip：{}", sourceZipUrl);
    }

    /**
     * 单独读取zip内单个entry字节，任务独立打开流，用完释放
     */
    private byte[] readSingleZipEntryBytes(WebdavDTO webdavDTO, UploadMetaTask metaTask) throws Exception {
        try (
                InputStream zipIs = getRawInputStream(webdavDTO, metaTask.sourceZipUrl);
                ZipArchiveInputStream zipIn = new ZipArchiveInputStream(zipIs, "GBK", true)
        ) {
            ZipArchiveEntry entry;
            ByteArrayOutputStream bos = new ByteArrayOutputStream(BUFFER_SIZE);
            byte[] buffer = new byte[BUFFER_SIZE];
            int len;
            while ((entry = zipIn.getNextZipEntry()) != null) {
                if (metaTask.entryRawName.equals(entry.getName())) {
                    while ((len = zipIn.read(buffer)) != -1) {
                        bos.write(buffer, 0, len);
                    }
                    return bos.toByteArray();
                }
            }
            throw new RuntimeException("压缩包内未匹配到目标条目：" + metaTask.entryRawName);
        }
    }

    /**
     * 获取原始S3输入流（不拼接dataset前缀，读取外部zip文件专用）
     */
    private InputStream getRawInputStreamByBucketName(WebdavDTO webdavDTO, String sourceBucketName,String rawObjPath) {
        S3Object s3Object = s3FileClient.getObject(sourceBucketName, rawObjPath);
        return s3Object.getObjectContent();
    }

    /**
     * 获取原始S3输入流（不拼接dataset前缀，读取外部zip文件专用）
     */
    private InputStream getRawInputStream(WebdavDTO webdavDTO, String rawObjPath) {
        S3Object s3Object = s3FileClient.getObject(s3FileClientConfig.getBucket(), rawObjPath);
        return s3Object.getObjectContent();
    }

}
