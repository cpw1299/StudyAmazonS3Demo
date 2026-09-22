package com.yhcx.framework.file.core.service;

import cn.hutool.core.io.FileUtil;
import cn.hutool.core.io.IoUtil;
import cn.hutool.core.util.CharsetUtil;
import cn.hutool.core.util.StrUtil;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.HttpMethod;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.*;
import com.github.junrar.exception.RarException;
import com.yhcx.framework.common.pojo.PageParam;
import com.yhcx.framework.common.pojo.PageResult;
import com.yhcx.framework.common.util.file.Tools;
import com.yhcx.framework.common.util.json.JsonUtils;
import com.yhcx.framework.common.util.object.PageUtils;
import com.yhcx.framework.file.core.constant.S3FileConstants;
import com.yhcx.framework.file.core.convert.S3Convert;
import com.yhcx.framework.file.core.dto.*;
import com.yhcx.framework.file.util.ArchiveUtils;
import com.yhcx.module.infra.api.file.dto.FilePresignedUrlRespDTO;
import com.yhcx.module.infra.framework.file.core.client.FileClient;
import com.yhcx.module.infra.framework.file.core.client.s3.S3FileClientConfig;
import com.yhcx.module.infra.framework.file.core.utils.FileTypeUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.tika.Tika;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.InputStreamResource;
import org.springframework.data.redis.connection.RedisZSetCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.ContentDisposition;
import org.springframework.http.HttpHeaders;
import org.springframework.util.CollectionUtils;
import org.springframework.web.multipart.MultipartFile;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.PreDestroy;
import javax.servlet.http.HttpServletResponse;
import javax.validation.constraints.NotBlank;
import java.io.*;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

public class S3FileStorageServiceImpl implements S3FileStorageService {

    private final Logger log = LoggerFactory.getLogger(S3FileStorageServiceImpl.class);

    //
    private static final long SHUTDOWN_AWAIT_SECONDS = 10L;

    //==================== S3 Multipart 分片相关常量 ====================
    /**
     * Multipart 默认分片大小：128MB，必须大于S3强制最小5MB   满足分片MAX_PART_COUNT=10000文件 超过1T文件
     */
    private static final long DEFAULT_PART_SIZE = 128L * 1024 * 1024;
    /**
     * S3分片最小分片大小，小于5MB服务端报错（最后一片除外）
     */
    private static final long MIN_PART_SIZE = 5L * 1024 * 1024;
    /**
     * S3 Multipart最大分片数量硬上限：10000
     */
    private static final int MAX_PART_COUNT = 10000;
    /**
     * 分片扩容补偿偏移量，防止计算出来分片大小刚好临界偏小
     */
    private static final long PART_SIZE_OFFSET = 10L * 1024 * 1024;

//==================== S3 CopyObject 拷贝相关常量 ====================
    /**
     * CopyObject接口直接拷贝最大阈值：5GB，S3原生CopyObject上限，超过必须Multipart‑Copy
     */
    private static final long MAX_DIRECT_COPY = 5L * 1024 * 1024 * 1024;

    // 分片上传单块大小 128MB 适配100G级超大文件
    private static final long PART_SIZE = 128 * 1024 * 1024L;
    // IO缓冲区 128KB（131072)
    private static final int BUFFER_SIZE = 16 * 8192;
    // 内存缓冲区大小，不足分片时缓存
    private static final int MEM_BUFFER = 2 * 1024 * 1024;

//==================== IO、本地磁盘相关常量 ====================
    /**
     * 文件读写缓冲区大小 128KB（131072)
     */
    private static final int IO_BUFFER_SIZE = BUFFER_SIZE;
    /**
     * 本地磁盘预校验预留系数，防止磁盘临界值瞬间写满
     */
    private static final double DISK_SPACE_RESERVE_RATIO = 1.2D;

    //==================== 文件列表分页缓存相关常量 ====================
    private static final int FILE_PAGE_SIZE = 15;

    private static final long FILE_LIST_CACHE_TTL = 30L * 60L;

    private final S3TempFileDownloader s3TempFileDownloader;

    private final StringRedisTemplate stringRedisTemplate;

    private final S3FileClientConfig s3FileClientConfig;

    private final AmazonS3Client s3FileClient;

    private final ZipToMinioService zipToMinioService;

    private final FileDownloadService fileDownloadService;

    //分批删除对象，强制每批最大1000，解决MalformedXML
    private static final int S3_DELETE_OBJECTS_MAX_BATCH = 1000;

    // 业务异步线程池
    private final ExecutorService executor = new ThreadPoolExecutor(
            10,
            20,
            60L, TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(200),
            new ThreadPoolExecutor.CallerRunsPolicy()
    );

    public S3FileStorageServiceImpl(FileClient fileClient,
                                    S3TempFileDownloader s3TempFileDownloader,
                                    ZipToMinioService zipToMinioService,
                                    FileDownloadService fileDownloadService,
                                    StringRedisTemplate stringRedisTemplate) {
        this.s3TempFileDownloader = s3TempFileDownloader;
        this.s3FileClientConfig = (S3FileClientConfig) fileClient.getFileConfig();
        this.s3FileClient = (AmazonS3Client) Objects.requireNonNull(fileClient).getClient();
        this.stringRedisTemplate = stringRedisTemplate;
        this.zipToMinioService = zipToMinioService;
        this.fileDownloadService = fileDownloadService;
    }

    /**
     * 获取原生S3客户端，供外部分片上传使用
     */
    @Override
    public AmazonS3Client getS3Client() {
        return this.s3FileClient;
    }

    /**
     * 【修复】销毁顺序：优先关闭业务线程池，等待任务收尾，再关闭S3客户端
     */
    @PreDestroy
    public void closeS3() {
        log.info("[S3FileStorageServiceImpl @PreDestroy] begin shutdown resource");
        // 1. 先关闭业务线程池，停止接收新任务
        executor.shutdown();
        try {
            // 等待已有任务执行完成，最多等待10秒
            if (!executor.awaitTermination(SHUTDOWN_AWAIT_SECONDS, TimeUnit.SECONDS)) {
                log.warn("[S3FileStorageServiceImpl @PreDestroy] executor awaitTermination timeout, force shutdownNow");
                executor.shutdownNow();
                // 再给一点时间中断
                if (!executor.awaitTermination(3, TimeUnit.SECONDS)) {
                    log.error("[S3FileStorageServiceImpl @PreDestroy] executor cannot terminate");
                }
            }
        } catch (InterruptedException e) {
            log.warn("[S3FileStorageServiceImpl @PreDestroy] shutdown executor interrupted", e);
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }

        // 2. 线程池处理完毕之后，再关闭AmazonS3Client
        if (s3FileClient != null) {
            s3FileClient.shutdown();
            log.info("[S3FileStorageServiceImpl @PreDestroy] AmazonS3Client shutdown done");
        }
        log.info("[S3FileStorageServiceImpl @PreDestroy] resource shutdown complete");
    }

    /**
     * S3协议（MinIO也遵循）通过 Delimiter 参数来区分:
     * objectSummaries (对象列表)：
     * - 返回的是具体的对象。
     * - 如果查询条件是 prefix=test/123/，且 Bucket 中确实存在一个 Key 为 test/123/ 的 0 byte 对象。
     * - 它是一个实实在在存储了的“文件”。
     * commonPrefixes (公共前缀/目录)：
     * - 返回的是推导出的“目录”，而非具体对象。
     * - 它是基于“路径层级”推导出来的。
     * - 典型场景：如果你有一个文件 test/123/photo.jpg。
     * - - 查询 prefix=test/, delimiter=/。
     * - - 结果：commonPrefixes 会包含 test/123/。
     * - - 注意：此时 Bucket 里可能根本没有 test/123/ 这个 0 byte 对象！S3/MinIO 仅仅因为存在更深层的文件，推导出了这个目录名。
     */
    @Override
    public Object listObjectsV2(String bucketName, String rootPath, String path, boolean delimiter) {
        String storagePath = Tools.join(false, rootPath, path);
        log.info("[Minio 操作路径] listObjectsV2：bucketName：{}，storagePath={}", bucketName, storagePath);
        ListObjectsV2Request listObjectsV2Request = new ListObjectsV2Request()
                .withBucketName(bucketName)
                .withMaxKeys(Integer.MAX_VALUE)
                .withPrefix(storagePath + "/");
        if (delimiter) {
            listObjectsV2Request.withDelimiter("/");
        }
        return s3FileClient.listObjectsV2(listObjectsV2Request);
    }

    @Override
    public Object getObject(String bucketName, String rootPath, String path) {
        String storagePath = rootPath + "/" + path;
        log.info("[Minio 操作路径] getObject：bucketName：{}，storagePath={}", bucketName, storagePath);
        GetObjectRequest objectRequest = new GetObjectRequest(bucketName, storagePath);
        return s3FileClient.getObject(objectRequest);
    }

    @Override
    public Object putObject(String bucketName, String rootPath, String path, Map<String, String> metadata) {
        String storagePath = Tools.join(false, rootPath, path);
        if ("/".equals(path) || path.endsWith("/")) {
            storagePath = Tools.appendSuffix(storagePath);
        }
        log.info("[Minio 操作路径] putObject：bucketName：{}，storagePath={}", bucketName, storagePath);
        // 元数据，主要用于设置文件类型
        ObjectMetadata objectMetadata = new ObjectMetadata();
        // 如果不设置，会有 “ No content length specified for stream data” 警告日志
        objectMetadata.setContentLength(0L);
        // 设置内容类型，表明这是一个目录
        objectMetadata.setContentType("application/x-directory");
        objectMetadata.setUserMetadata(metadata);
        // 构建请求对象
        PutObjectRequest request = new PutObjectRequest(
                bucketName,
                storagePath,
                new ByteArrayInputStream(new byte[0]),
                objectMetadata
        );
        // 执行上传
        return s3FileClient.putObject(request);
    }

    @Override
    public InputStream downloadAsStream(String bucket, String rootPath, String path) throws IOException {
        String storagePath = Tools.join(false, rootPath, path);
        return s3TempFileDownloader.downloadToTempStream(bucket, storagePath);
    }


    @Override
    public PageResult<FileRespDTO> filePageList(String bucketName, String rootPath, String path, Boolean metadata, Integer pageNo) {
        String storagePath = Tools.join(false, rootPath, path);
        log.info("[Minio 操作路径] filePageList：bucketName：{}，storagePath={}", bucketName, storagePath);
        String cacheKey = String.format("minio:file:list:%s:%s", bucketName, storagePath);

        // 先尝试从缓存读取
        Long total = stringRedisTemplate.opsForList().size(cacheKey);
        if (total == null || total == 0) {
            // 缓存不存在，从 S3 获取完整列表（含元数据）并缓存
            List<FileRespDTO> fileList = this.fileListAllWithMetadata(bucketName, rootPath, path, metadata);
            if (fileList == null || fileList.isEmpty()) {
                return new PageResult<>(new ArrayList<>(), 0L);
            }
            List<String> jsonList = new ArrayList<>(fileList.size());
            for (FileRespDTO dto : fileList) {
                jsonList.add(JsonUtils.toJsonString(dto));
            }
            stringRedisTemplate.delete(cacheKey);
            stringRedisTemplate.opsForList().rightPushAll(cacheKey, jsonList);
            stringRedisTemplate.expire(cacheKey, FILE_LIST_CACHE_TTL, TimeUnit.SECONDS);
            total = (long) fileList.size();
        }

        // 分页读取
        PageParam pageParam = new PageParam();
        pageParam.setPageSize(FILE_PAGE_SIZE);
        pageParam.setPageNo(pageNo);
        long start = PageUtils.getStart(pageParam);
        long end = start + FILE_PAGE_SIZE - 1;
        List<String> jsonList = stringRedisTemplate.opsForList().range(cacheKey, start, end);

        List<FileRespDTO> result = new ArrayList<>();
        if (jsonList != null) {
            for (String json : jsonList) {
                result.add(JsonUtils.parseObject(json, FileRespDTO.class));
            }
        }
        return new PageResult<>(result, total);
    }

    @Override
    public List<FileRespDTO> fileListAll(String bucketName, String rootPath, String path) {
        String storagePath = Tools.join(false, rootPath, path);
        log.info("[Minio 操作路径] fileListAll：bucketName：{}，storagePath={}", bucketName, storagePath);

        S3ObjectResult objectResult = this.whileGetListObjectsV2(bucketName, storagePath, true);
        List<S3ObjectSummary> objectSummaries = objectResult.getObjectSummaries();
        List<String> commonPrefixes = objectResult.getCommonPrefixes();

        List<FileRespDTO> fileList = new ArrayList<>();
        if (!commonPrefixes.isEmpty()) {
            S3Convert.listFolder(rootPath, path, storagePath, commonPrefixes, fileList);
        }
        if (!objectSummaries.isEmpty()) {
            S3Convert.listFile(rootPath, path, storagePath, objectSummaries, fileList);
        }
        return fileList;
    }

    @Override
    public List<FileRespDTO> fileListAllWithMetadata(String bucketName, String rootPath, String path, boolean metadata) {
        String storagePath = Tools.appendSuffix(Tools.join(false, rootPath, path));
        log.info("[Minio 操作路径] fileListAllWithMetadata：bucketName：{}，storagePath={}", bucketName, storagePath);

        S3ObjectResult objectResult = this.whileGetListObjectsV2(bucketName, storagePath, true);
        List<S3ObjectSummary> objectSummaries = objectResult.getObjectSummaries();
        List<String> commonPrefixes = objectResult.getCommonPrefixes();
        List<FileRespDTO> fileList = new ArrayList<>();
        if (!commonPrefixes.isEmpty()) {
            S3Convert.listFolder(rootPath, path, storagePath, commonPrefixes, fileList);
        }
        // 判断 入参 path 的父级path 是否存在 “datasetId” 标识，如果父级存在表标识，子级必然不允许存在标识
        boolean parentFlag = this.existsPrefix(bucketName, storagePath);
        // 未处理 objectSummaries 之前，如果需要处理 Metadata 元数据
        if (metadata && !fileList.isEmpty() && !parentFlag) {
            // 创建任务列表
            List<CompletableFuture<Void>> futureList = new ArrayList<>();
            for (FileRespDTO dto : fileList) {
                CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                    try {
                        // 关键：查询对象的 Metadata (HEAD 请求)
                        // MinIO/S3 中，文件夹对象本身就是一个以 "/" 结尾的 Key
                        ObjectMetadata objectMetadata = s3FileClient.getObjectMetadata(bucketName, dto.getOriginalPath());
                        // 获取自定义属性
                        Map<String, String> userMeta = objectMetadata.getUserMetadata();
                        if (userMeta == null) {
                            dto.putMetadata(S3FileConstants.FRONT_DATASET_ID, null);
                        } else {
                            // 注意：SDK 返回的 Map 中 key 是小写的
                            dto.putMetadata(S3FileConstants.FRONT_DATASET_ID, userMeta.get(S3FileConstants.STORE_DATASET_ID));
                        }
                    } catch (AmazonS3Exception e) {
                        CompletableFuture.runAsync(() -> {
                            if (dto.getOriginalPath().endsWith("/") && "404 Not Found".equals(e.getErrorCode())) {
                                ObjectMetadata objectMetadata = new ObjectMetadata();
                                objectMetadata.setContentLength(0L);
                                objectMetadata.setContentType("application/x-directory");
                                s3FileClient.putObject(new PutObjectRequest(
                                        bucketName,
                                        dto.getOriginalPath(),
                                        new ByteArrayInputStream(new byte[0]),
                                        objectMetadata
                                ));
                            }
                        });
                    }
                }, executor);
                futureList.add(future);
            }
            futureList.forEach(CompletableFuture::join);
            // 将 List 转为数组
            CompletableFuture<Void> allFutures = CompletableFuture.allOf(
                    futureList.toArray(new CompletableFuture[0])
            );
            // 阻塞主线程，等待所有任务结束
            try {
                // join() 和 get() 的区别在于 get() 需要抛出受检异常，join() 抛出的是未检查异常，代码写起来更简洁。
                allFutures.get(); // 或者 allFutures.join()
            } catch (InterruptedException | ExecutionException e) {
                log.error("[Minio 操作路径] fileListAllWithMetadata：查询源数据报错 bucketName：{}，storagePath={}", bucketName, storagePath, e);
            }
            // 判断
        }
        if (!fileList.isEmpty()) {
            if (parentFlag) {
                for (FileRespDTO dto : fileList) {
                    // 跟前端约定：父级path存在 “datasetId” 标识
                    dto.putMetadata(S3FileConstants.FRONT_DATASET_ID, "-9999");
                }
            } else {
                // 父级path 不存在 “datasetId” 标识时，本级去查询了 getObjectMetadata，将有“datasetId” 标识的path存入缓存
                for (FileRespDTO dto : fileList) {
                    if (dto.getMetadata() != null && dto.getMetadata().get(S3FileConstants.FRONT_DATASET_ID) != null) {
                        this.addPrefix(bucketName, dto.getOriginalPath());
                    }
                }
            }
        }
        if (!objectSummaries.isEmpty()) {
            S3Convert.listFile(rootPath, path, storagePath, objectSummaries, fileList);
        }
        return fileList;
    }

    private String getRedisKey(String bucketName, String prefix) {
        String[] split = prefix.split("/");
        List<String> parts = new ArrayList<>();
        parts.add(bucketName);
        for (int i = 0; i < 4; i++) {
            if (i < split.length) {
                parts.add(split[i]);
            } else {
                parts.add("");
            }
        }
        return "minio_" + String.join(":", parts);
    }


    public void addPrefix(String bucketName, String prefix) {
        // Minio 的 Key 全路径是 dataset/10/2120/4278/0806数据集2200/... 无限长
        String key = this.getRedisKey(bucketName, prefix);
        // 1. 向 ZSet 添加元素 (score 为 0)
        Boolean added = stringRedisTemplate.opsForZSet().add(key, prefix, 0);
        // 2. 设置 Key 的过期时间
        if (Boolean.TRUE.equals(added)) {
            // 如果 Key 是新增的，或者你希望每次操作都刷新过期时间，都可以调用这行
            stringRedisTemplate.expire(key, 30, TimeUnit.MINUTES);
        }
    }

    public boolean existsPrefix(String bucketName, String query) {
        // Minio 的 Key 全路径是 dataset/10/2120/4278/0806数据集2200/... 无限长
        String key = this.getRedisKey(bucketName, query);
        Set<String> values = stringRedisTemplate.opsForZSet().reverseRangeByLex(
                key,
                RedisZSetCommands.Range.range().lte(query),
                RedisZSetCommands.Limit.limit().count(10)
        );
        if (values == null || values.isEmpty()) {
            return false;
        }
        String prefix = values.iterator().next();
        return query.startsWith(prefix);
    }


    @Override
    public List<FileRespDTO> fileDirectList(String bucketName, String rootPath, String path) {
        String storagePath = Tools.join(false, rootPath, path);
        log.info("[Minio 操作路径] fileDirectList：bucketName：{}，storagePath={}", bucketName, storagePath);

        List<S3ObjectSummary> s3ObjectSummaries = new ArrayList<>();
        ListObjectsV2Request listObjectsRequest = new ListObjectsV2Request()
                .withBucketName(bucketName)
                .withPrefix(storagePath + "/");
        ListObjectsV2Result result;
        do {
            result = s3FileClient.listObjectsV2(listObjectsRequest);
            s3ObjectSummaries.addAll(result.getObjectSummaries());
            String token = result.getNextContinuationToken();
            listObjectsRequest.setContinuationToken(token);
        } while (result.isTruncated());

        List<FileRespDTO> fileList = new ArrayList<>();
        S3Convert.listFile(storagePath, path, s3ObjectSummaries, fileList);
        return fileList;
    }

    /**
     * 获取文件夹列表及其 FolderType 属性
     */
    public Map<String, String> listFoldersWithMetadata(String bucketName, String parentPath) {
        // 1. 列出 commonPrefixes
        ListObjectsV2Request listRequest = new ListObjectsV2Request()
                .withBucketName(bucketName)
                .withPrefix(parentPath)
                .withDelimiter("/"); // 关键：使用分隔符才能获取文件夹模式
        ListObjectsV2Result listResult = s3FileClient.listObjectsV2(listRequest);
        List<String> commonPrefixes = listResult.getCommonPrefixes();
        // 2. 并发查询每个文件夹的元数据
        // Map: Key=文件夹名, Value=FolderType值
        Map<String, String> resultMap = new ConcurrentHashMap<>();
        // 创建任务列表
        List<Callable<Void>> tasks = commonPrefixes.stream().map(prefix -> {
            return (Callable<Void>) () -> {
                try {
                    // 关键：查询对象的 Metadata (HEAD 请求)
                    // MinIO/S3 中，文件夹对象本身就是一个以 "/" 结尾的 Key
                    ObjectMetadata metadata = s3FileClient.getObjectMetadata(bucketName, prefix);

                    // 获取自定义属性
                    Map<String, String> userMeta = metadata.getUserMetadata();
                    if (userMeta != null) {
                        // 注意：SDK 返回的 Map 中 key 是小写的
                        String folderType = userMeta.get("foldertype");
                        resultMap.put(prefix, folderType != null ? folderType : "unknown");
                    } else {
                        resultMap.put(prefix, "unknown");
                    }
                } catch (Exception e) {
                    // 如果文件夹对象不存在（可能是通过子文件推断出来的文件夹，而非手动创建的虚拟文件夹）
                    resultMap.put(prefix, "no-object");
                }
                return null;
            };
        }).collect(Collectors.toList());
        try {
            // 等待所有查询完成，设置超时时间防止死锁
            executor.invokeAll(tasks, 10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return resultMap;
    }


    public S3ObjectResult whileGetListObjectsV2(String bucketName, String storagePath, boolean delimiter) {
        List<S3ObjectSummary> objectSummaries = new ArrayList<>();
        List<String> commonPrefixes = new ArrayList<>();
        // 构建请求
        ListObjectsV2Request request = new ListObjectsV2Request()
                .withBucketName(bucketName)
                .withPrefix(storagePath);
        if (delimiter) {
            request.withDelimiter("/");
        }
        log.info("[Minio 操作路径] whileGetListObjectsV2：bucketName：{}，storagePath={}", bucketName, storagePath);
        String continuationToken = null;
        boolean isTruncated;
        // 循环分页查询
        do {
            // 如果有下一页的 token，设置到请求中
            if (continuationToken != null) {
                request.setContinuationToken(continuationToken);
            }
            // 执行查询
            ListObjectsV2Result v2Result = s3FileClient.listObjectsV2(request);
            // // 4. 处理结果，处理当前页的结果
            // v2Result.getObjectSummaries() 返回当前页的文件列表
            if (v2Result.getObjectSummaries() != null) {
                objectSummaries.addAll(v2Result.getObjectSummaries());
            }
            // v2Result.getCommonPrefixes() 返回当前目录下的子目录列表（如果使用了 delimiter）
            if (v2Result.getCommonPrefixes() != null) {
                commonPrefixes.addAll(v2Result.getCommonPrefixes());
            }
            // 检查是否还有下一页
            isTruncated = v2Result.isTruncated();
            if (isTruncated) {
                // 获取下一页的 token
                continuationToken = v2Result.getNextContinuationToken();
            }
        } while (isTruncated);
        return new S3ObjectResult(objectSummaries, commonPrefixes);
    }

    private List<S3ObjectSummary> getS3ObjectSummaries(String bucketName, String storagePath) {
        List<S3ObjectSummary> s3ObjectSummaries = new ArrayList<>();
        ListObjectsV2Request listObjectsRequest = new ListObjectsV2Request()
                .withBucketName(bucketName)
                .withPrefix(Tools.appendSuffix(storagePath));
        ListObjectsV2Result result;
        do {
            result = s3FileClient.listObjectsV2(listObjectsRequest);
            s3ObjectSummaries.addAll(result.getObjectSummaries());
            String token = result.getNextContinuationToken();
            listObjectsRequest.setContinuationToken(token);
        } while (result.isTruncated());
        return s3ObjectSummaries;
    }

    @Override
    public List<FolderRespDTO> folderList(String bucketName, String rootPath, String path) {
        return folderList(bucketName, rootPath, path, true);
    }

    @Override
    public List<FolderRespDTO> folderList(String bucketName, String rootPath, String path, Boolean metadata) {
        String storagePath = Tools.join(false, rootPath, path);
        log.info("[Minio 操作路径] folderList：bucketName：{}，storagePath={}", bucketName, storagePath);
        S3ObjectResult objectResult = this.whileGetListObjectsV2(bucketName, storagePath, false);
        List<S3ObjectSummary> objectSummaries = objectResult.getObjectSummaries();
        List<String> commonPrefixes = objectResult.getCommonPrefixes();
        List<String> objectSummaryKeys = objectSummaries.stream().map(S3ObjectSummary::getKey).collect(Collectors.toList());
        if (!metadata && !objectSummaryKeys.isEmpty()) {
            Map<String, String> metadataMap = new HashMap<>();
            List<String> metadataList = new ArrayList<>();
            List<CompletableFuture<Void>> futureList = new ArrayList<>();
            for (String objectSummaryKey : objectSummaryKeys) {
                if (!objectSummaryKey.endsWith("/")) {
                    continue;
                }
                CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                    try {
                        // 关键：查询对象的 Metadata (HEAD 请求)
                        // MinIO/S3 中，文件夹对象本身就是一个以 "/" 结尾的 Key
                        ObjectMetadata objectMetadata = s3FileClient.getObjectMetadata(bucketName, objectSummaryKey);
                        // 获取自定义属性
                        Map<String, String> userMeta = objectMetadata.getUserMetadata();
                        String metadataValue = userMeta.get(S3FileConstants.STORE_DATASET_ID);
                        if (metadataValue != null && !metadataValue.isBlank()) {
                            metadataMap.put(objectSummaryKey, metadataValue);
                            metadataList.add(objectSummaryKey);
                        }
                    } catch (AmazonS3Exception ignore) {
                    }
                }, executor);
                futureList.add(future);
            }
            // 将 List 转为数组
            CompletableFuture<Void> allFutures = CompletableFuture.allOf(
                    futureList.toArray(new CompletableFuture[0])
            );
            // 阻塞主线程，等待所有任务结束
            try {
                // join() 和 get() 的区别在于 get() 需要抛出受检异常，join() 抛出的是未检查异常，代码写起来更简洁。
                allFutures.get(); // 或者 allFutures.join()
            } catch (InterruptedException | ExecutionException e) {
                log.error("[Minio 操作路径] folderList：查询源数据报错 bucketName：{}，storagePath={}", bucketName, storagePath, e);
            }
            // 移除掉 userMetadata 中有数据集标记的
            if (!metadataList.isEmpty()) {
                for (int i = objectSummaryKeys.size() - 1; i > 0; i--) {
                    String key = objectSummaryKeys.get(i);
                    if (metadataList.contains(key)) {
                        objectSummaryKeys.remove(i);
                        continue;
                    }
                    for (String prefix : metadataList) {
                        if (key.startsWith(prefix)) {
                            objectSummaryKeys.remove(i);
                            break;
                        }
                    }
                }
            }
        }
        return S3Convert.listFolderRecursively(storagePath, path, objectSummaries);
    }

    @Override
    public FileDTO fileCountSize(String bucketName, String rootPath, String path) {
        // 1. 获取文件列表，防御性处理空指针
        List<FileRespDTO> fileList = fileDirectListToDataset(bucketName, rootPath, path);
        if (CollectionUtils.isEmpty(fileList)) {
            return new FileDTO(0L, 0L);
        }

        // 2. 流式统计：仅计算size非空且非0的文件数量和总大小
        AtomicLong totalFileCount = new AtomicLong(0L);
        AtomicLong totalFileSize = new AtomicLong(0L);

        fileList.stream()
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

    private List<FileRespDTO> fileDirectListToDataset(String bucketName, String rootPath, @NotBlank String path) {
        String storagePath = Tools.join(false, rootPath, path);
        log.info("[Minio 操作路径] fileDirectListToDataset：bucketName：{}，storagePath={}", bucketName, storagePath);
        S3ObjectResult objectResult = this.whileGetListObjectsV2(bucketName, storagePath, false);
        List<S3ObjectSummary> objectSummaries = objectResult.getObjectSummaries();
        List<String> commonPrefixes = objectResult.getCommonPrefixes();
        List<FileRespDTO> fileList = new ArrayList<>();
        if (!objectSummaries.isEmpty()) {
            S3Convert.listFile(storagePath, path, objectSummaries, fileList);
        }
        return fileList;
    }

    @Override
    public String upload(String bucketName, String rootPath, String path, MultipartFile file) throws IOException, RarException {
        byte[] content = file.getBytes();
        boolean compressed = isCompressedByTika(content);
        if (compressed) {
            this.uploadZip(bucketName, rootPath, path, file);
            return "";
        } else {
            return this.upload(bucketName, rootPath, path, content);
        }
    }

    /**
     * 文件解压并上传 解压包会存在特别大文件 比例：test/test.zip 100G  解压后的文件也可以存储大文件 100G
     * 解压包  把压缩包对象上传到目标桶，解压包对象解压并上传到目标桶 只解压ZIP
     *
     * @param bucketName       桶名称
     * @param rootPath         根目录
     * @param extractPath      提取路径  解压包会存在特别大文件 比例：test/test.zip 100G  解压后的文件也可以存储大文件 100G
     * @param targetBucketName 目标桶名称
     * @param targetFolderPath 目标文件夹路径
     * @return 文件路径
     */
    @Override
    public String zipPathFileExtract(String bucketName, String rootPath, String extractPath,
                                     String targetBucketName, String targetFolderPath) {
        // 拼接源桶zip完整对象key
        String zipObjKey = safeJoinS3Path(rootPath, extractPath);
        log.info("开始超大ZIP流式解压上传，源桶:{} zipKey:{} 目标桶:{} 解压前缀:{}",
                bucketName, zipObjKey, targetBucketName, targetFolderPath);
        // 2. 流式拉取S3 zip输入流，不下载完整文件到本地磁盘
        try (
                InputStream rawIn = downloadAsStream(bucketName, rootPath, extractPath);
                BufferedInputStream bufferedIn = new BufferedInputStream(rawIn, BUFFER_SIZE)) {
            ZipInputStream zipIn;
            try {
                zipIn = new ZipInputStream(bufferedIn, CharsetUtil.CHARSET_GBK);
            } catch (Exception e) {
                log.warn("GBK解析ZIP失败，切换UTF-8编码，err:{}", e.getMessage());
                zipIn = new ZipInputStream(bufferedIn, CharsetUtil.CHARSET_UTF_8);
            }

            ZipEntry entry;
            // 遍历压缩包内所有条目
            while ((entry = zipIn.getNextEntry()) != null) {
                String entryName = entry.getName();
                log.debug("读取压缩包原始条目名称:{}", entryName);
                // 安全校验：拦截路径穿越漏洞 ../../etc/passwd
                if (isPathTraversal(entryName)) {
                    log.error("检测到路径穿越风险，跳过条目:{}", entryName);
                    zipIn.closeEntry();
                    continue;
                }
                // 目录，无需上传，仅创建目录标记(S3无目录，可跳过)
                if (entry.isDirectory()) {
                    log.debug("跳过压缩包目录:{}", entryName);
                    zipIn.closeEntry();
                    continue;
                }

                // ========== 核心修复：清洗文件名，消除非法字符 ==========
                String cleanEntryName = cleanS3ObjectName(entryName);
                // 拼接目标桶完整对象key（安全路径拼接，无连续斜杠）
                String targetObjKey = safeJoinS3Path(targetFolderPath, cleanEntryName);
                log.info("开始解压上传文件 原始名称:{} -> 清洗后key:{}", entryName, targetObjKey);

                try {
                    // 流式分片上传当前解压文件到目标S3桶
                    streamUploadToS3(targetBucketName, targetObjKey, zipIn);
                } catch (AmazonServiceException e) {
                    // 单文件上传失败不中断整体解压，记录错误跳过当前文件
                    log.error("单文件上传失败，跳过该文件 rawName:{} targetKey:{} code:{} msg:{}",
                            entryName, targetObjKey, e.getErrorCode(), e.getMessage(), e);
                    abortMultipartUpload(targetBucketName, targetObjKey, e.getRequestId());
                } catch (Exception e) {
                    log.error("文件分片上传异常，跳过 rawName:{} targetKey:{}", entryName, targetObjKey, e);
                } finally {
                    zipIn.closeEntry();
                }
            }
            zipIn.close();
        } catch (AmazonServiceException e) {
            log.error("S3服务异常，解压上传整体失败 bucket:{} key:{} code:{} msg:{}",
                    bucketName, zipObjKey, e.getErrorCode(), e.getMessage(), e);
            throw new RuntimeException("S3存储服务异常:" + e.getErrorCode(), e);
        } catch (IOException e) {
            log.error("压缩包读取/IO流异常 key:{}", zipObjKey, e);
            throw new RuntimeException("ZIP文件读取异常", e);
        }
        log.info("ZIP解压上传全部完成，目标根目录:{}", targetFolderPath);
        return targetFolderPath;
    }

    /**
     * 清洗S3对象Key，移除MinIO不支持的非法字符，解决 XMinioInvalidObjectName
     * 禁止字符：\ : * ? " < > | ；空格替换为下划线，清理首尾空白、反斜杠转正斜杠
     *
     * @param rawName zip原始文件名/路径
     * @return 合规S3 object key片段
     */
    private String cleanS3ObjectName(String rawName) {
        if (StrUtil.isBlank(rawName)) {
            return rawName;
        }
        String clean = rawName.trim();
        // 新增：移除回车、换行，避免生成XML非法key
        clean = clean.replaceAll("[\\r\\n]", "_");
        // Windows非法文件名字符全部替换为下划线
        clean = clean.replaceAll("[\\\\:*?\"<>|]", "_");
        clean = clean.replaceAll("\\s+", "_");
        clean = clean.replace("\\", "/");
        while (clean.contains("//")) {
            clean = clean.replace("//", "/");
        }
        return clean;
    }


    /**
     * 安全拼接S3路径，避免连续斜杠
     */
    private String safeJoinS3Path(String... paths) {
        List<String> validParts = new ArrayList<>();
        for (String path : paths) {
            if (StrUtil.isNotBlank(path)) {
                validParts.add(path.replaceAll("/+$", ""));
            }
        }
        return String.join("/", validParts);
    }

    /**
     * 路径穿越检测，防止../逃逸、绝对路径、非法前缀
     */
    private boolean isPathTraversal(String entryName) {
        if (StrUtil.isBlank(entryName)) {
            return true;
        }
        // 包含 ../ 、 ./ 、 开头/、开头\
        return entryName.contains("..")
                || entryName.startsWith("/")
                || entryName.startsWith("\\")
                || entryName.startsWith("./");
    }

    /**
     * 流式分片上传解压后的文件到目标S3桶（适配超大文件）
     *
     * @param targetBucket   目标桶
     * @param targetKey      目标对象key
     * @param zipInputStream zip条目输入流
     */
    private void streamUploadToS3(String targetBucket, String targetKey, InputStream zipInputStream) throws IOException {
        // 1. 初始化分片上传任务
        InitiateMultipartUploadRequest initReq = new InitiateMultipartUploadRequest(targetBucket, targetKey);
        InitiateMultipartUploadResult initResult = s3FileClient.initiateMultipartUpload(initReq);
        String uploadId = initResult.getUploadId();
        List<PartETag> partETagList = new ArrayList<>();
        int partNum = 1;
        byte[] buffer = new byte[MEM_BUFFER];
        int readLen;

        try (ByteArrayOutputStream partBuffer = new ByteArrayOutputStream()) {
            // 循环读取zip流，攒够分片大小上传
            while ((readLen = zipInputStream.read(buffer)) != -1) {
                partBuffer.write(buffer, 0, readLen);
                // 达到分片阈值，执行分片上传
                if (partBuffer.size() >= PART_SIZE) {
                    uploadSinglePart(targetBucket, targetKey, uploadId, partNum++, partBuffer, partETagList);
                    partBuffer.reset();
                }
            }
            // 处理剩余不足一个分片的数据
            if (partBuffer.size() > 0) {
                uploadSinglePart(targetBucket, targetKey, uploadId, partNum++, partBuffer, partETagList);
            }
        } catch (Exception e) {
            // 异常终止分片上传，清理碎片
            abortMultipartUpload(targetBucket, targetKey, uploadId);
            throw new IOException("分片上传文件失败 key:" + targetKey, e);
        }
        // 合并所有分片，完成上传
        CompleteMultipartUploadRequest completeReq = new CompleteMultipartUploadRequest()
                .withBucketName(targetBucket)
                .withKey(targetKey)
                .withUploadId(uploadId)
                .withPartETags(partETagList);
        s3FileClient.completeMultipartUpload(completeReq);
        log.info("文件分片上传完成 targetKey:{} 总分片数:{}", targetKey, partETagList.size());
    }

    /**
     * 上传单个分片
     */
    private void uploadSinglePart(String bucket, String key, String uploadId, int partNum,
                                  ByteArrayOutputStream partBuf, List<PartETag> eTagList) throws IOException {
        UploadPartRequest partReq = new UploadPartRequest()
                .withBucketName(bucket)
                .withKey(key)
                .withUploadId(uploadId)
                .withPartNumber(partNum)
                .withInputStream(IoUtil.toStream(partBuf.toByteArray()))
                .withPartSize(partBuf.size());
        UploadPartResult uploadResult = s3FileClient.uploadPart(partReq);
        eTagList.add(uploadResult.getPartETag());
    }

    /**
     * 异常时终止分片上传，释放碎片
     */
    private void abortMultipartUpload(String bucket, String key, String uploadId) {
        try {
            AbortMultipartUploadRequest abortReq = new AbortMultipartUploadRequest(bucket, key, uploadId);
            s3FileClient.abortMultipartUpload(abortReq);
            log.warn("分片上传异常，已终止上传碎片 key:{} uploadId:{}", key, uploadId);
        } catch (Exception ex) {
            log.error("终止分片上传失败 key:{} uploadId:{}", key, uploadId, ex);
        }
    }

    public static boolean isCompressedByTika(byte[] content) {
        Tika tika = new Tika();
        String mimeType = tika.detect(content);
        System.out.println(mimeType);
        return mimeType.equals("application/zip") ||
                mimeType.startsWith("application/x-rar-compressed") ||
                mimeType.equals("application/x-7z-compressed") ||
                mimeType.equals("application/gzip") ||
                mimeType.equals("application/x-tar");
    }

    @Override
    public String upload(String bucketName, String rootPath, String path, byte[] content) {
        String storagePath = Tools.join(false, rootPath, path);
        log.info("[Minio 操作路径] upload：bucketName：{}，storagePath={}", bucketName, storagePath);
        // 元数据，主要用于设置文件类型
        ObjectMetadata objectMetadata = new ObjectMetadata();
        // 计算默认的 path 名
        String type = FileTypeUtils.getMimeType(content);
        objectMetadata.setContentType(type);
        // 如果不设置，会有 “ No content length specified for stream data” 警告日志
        objectMetadata.setContentLength(content.length);
        // 执行上传
        s3FileClient.putObject(bucketName, storagePath, new ByteArrayInputStream(content), objectMetadata);
        // 拼接返回路径
        return "/" + path;
    }

    @Override
    public void uploadZip(String bucketName, String rootPath, String path, MultipartFile file) throws IOException, RarException {
        String storagePath = Tools.join(false, rootPath, path);
        log.info("[Minio 操作路径] uploadZip：bucketName：{}，storagePath={}", bucketName, storagePath);

        ArchiveResult archiveResult = ArchiveUtils.readArchive(file);
        List<String> folderList = archiveResult.getFolderList();
        List<FileUploadDTO> fileList = archiveResult.getFileList();

        List<CompletableFuture<Void>> futureList = new ArrayList<>();
        if (!fileList.isEmpty()) {
            for (FileUploadDTO dto : fileList) {
                CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                    // 元数据，主要用于设置文件类型
                    ObjectMetadata objectMetadata = new ObjectMetadata();
                    // 计算默认的 path 名
                    String type = FileTypeUtils.getMimeType(dto.getContent());
                    objectMetadata.setContentType(type);
                    // 如果不设置，会有 “ No content length specified for stream data” 警告日志
                    objectMetadata.setContentLength(dto.getSize());
                    // 执行上传
                    String zipFilePath = Tools.join(false, storagePath, dto.getFilePath());
                    log.info("[Minio 操作路径] uploadZip：bucketName：{}，zipFilePath={}", bucketName, zipFilePath);
                    s3FileClient.putObject(bucketName, zipFilePath, new ByteArrayInputStream(dto.getContent()), objectMetadata);
                }, executor);
                futureList.add(future);
            }
        }
        // 将 List 转为数组
        CompletableFuture<Void> allFutures = CompletableFuture.allOf(
                futureList.toArray(new CompletableFuture[0])
        );
        // 阻塞主线程，等待所有任务结束
        try {
            // join() 和 get() 的区别在于 get() 需要抛出受检异常，join() 抛出的是未检查异常，代码写起来更简洁。
            allFutures.get(); // 或者 allFutures.join()
        } catch (InterruptedException | ExecutionException e) {
            log.error("[Minio 操作路径] fileListAllWithMetadata：查询源数据报错 bucketName：{}，storagePath={}", bucketName, storagePath, e);
        }
    }

    @Override
    public String uploadUrl(String bucketName, String rootPath, String path, byte[] content) {
        return s3FileClientConfig.getDomain() + upload(bucketName, rootPath, path, content);
    }

    private String getDomain(String urlStr) {
        URI uri = URI.create(urlStr);
        String scheme = uri.getScheme();        // http
        String host = uri.getHost();            // 127.0.0.1
        int port = uri.getPort();               // 48080
        // 拼接：默认端口可以省略
        if (port == -1) {
            return scheme + "://" + host;
        } else {
            return scheme + "://" + host + ":" + port;
        }
    }

    @Override
    public FilePresignedUrlRespDTO getFilePresignedUrl(String bucketName, String rootPath, String path) {
        String storagePath = Tools.join(false, rootPath, path);
        log.info("[Minio 操作路径] getFilePresignedUrl：bucketName：{}，storagePath={}", bucketName, storagePath);
        // 设定过期时间为 10 分钟。取值范围：1 秒 ~ 7 天
        GeneratePresignedUrlRequest request = new GeneratePresignedUrlRequest(bucketName, storagePath, HttpMethod.GET);
        request.setExpiration(new Date(System.currentTimeMillis() + TimeUnit.DAYS.toMillis(7)));
        // 生成上传 URL
        URL url = s3FileClient.generatePresignedUrl(request);
        return new FilePresignedUrlRespDTO(
                url.getFile(),
                url.toString(),
                Tools.join(s3FileClientConfig.getDomain(), storagePath));
    }

    @Override
    public FilePresignedUrlRespDTO putFilePresignedUrl(String bucketName, String rootPath, String path) {
        String storagePath = Tools.join(false, rootPath, path);
        log.info("[Minio 操作路径] putFilePresignedUrl：bucketName：{}，storagePath={}", bucketName, storagePath);
        // 设定过期时间为 10 分钟。取值范围：1 秒 ~ 7 天
        GeneratePresignedUrlRequest request = new GeneratePresignedUrlRequest(bucketName, storagePath, HttpMethod.PUT);
        request.setExpiration(new Date(System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(10)));
        request.setContentType("application/octet-stream");
        // 生成上传 URL
        URL url = s3FileClient.generatePresignedUrl(request);
        // getDomain(s3FileClientConfig.getDomain()) + url.getFile()
        return new FilePresignedUrlRespDTO(url.getFile(),
                getDomain(s3FileClientConfig.getDomain()) + storagePath, null);
    }

    /**
     * 流式上传文件：小文件使用 PutObject，大文件使用 Multipart Upload。
     *
     * <p>该方法直接消费调用方提供的 InputStream，不会把整个文件落到本地磁盘。
     * Multipart 模式按分片读取输入流，每次只在内存中保留当前分片。</p>
     */
    @Override
    public void upload(String bucketName, String rootPath, String path,
                       InputStream inputStream, long contentLength, String contentType,
                       Map<String, String> metadata) throws IOException {
        if (StrUtil.isBlank(bucketName)) {
            throw new IllegalArgumentException("bucketName不能为空");
        }
        if (inputStream == null) {
            throw new IllegalArgumentException("inputStream不能为空");
        }
        if (contentLength < 0) {
            throw new IllegalArgumentException("contentLength不能小于0");
        }

        String storagePath = Tools.join(false, rootPath, path);
        if (StrUtil.isBlank(storagePath)) {
            throw new IllegalArgumentException("文件路径不能为空");
        }

        // S3 单次 PutObject 的对象大小上限为 5GB；超过该值必须使用 Multipart Upload。
        if (contentLength <= MAX_DIRECT_COPY) {
            ObjectMetadata objectMetadata = buildStreamUploadMetadata(contentLength, contentType, metadata);
            s3FileClient.putObject(new PutObjectRequest(
                    bucketName, storagePath, inputStream, objectMetadata));
            log.info("[S3流式上传] PutObject完成 bucket={},key={},size={}",
                    bucketName, storagePath, contentLength);
            return;
        }

        uploadLargeStreamMultipart(bucketName, storagePath, inputStream,
                contentLength, contentType, metadata);
    }

    /**
     * 大文件 Multipart Upload。
     *
     * <p>默认 128MB 一个 part；如果文件超过 128MB * 10000，自动增大 part，
     * 保证分片数量不超过 S3 的 10000 上限。每个 part 使用独立 byte[]，
     * 上传完成后立即复用该缓冲区，不创建本地临时文件。</p>
     */
    private void uploadLargeStreamMultipart(String bucketName, String storagePath,
                                            InputStream inputStream, long contentLength,
                                            String contentType, Map<String, String> metadata)
            throws IOException {
        long partSize = calculateMultipartPartSize(contentLength);
        InitiateMultipartUploadRequest initRequest = new InitiateMultipartUploadRequest(
                bucketName, storagePath);
        initRequest.setObjectMetadata(buildStreamUploadMetadata(null, contentType, metadata));

        String uploadId = null;
        List<PartETag> partETags = new ArrayList<>();
        long uploadedBytes = 0L;
        int partNumber = 1;

        try {
            uploadId = s3FileClient.initiateMultipartUpload(initRequest).getUploadId();
            log.info("[S3大文件Multipart] 初始化成功 bucket={},key={},uploadId={},partSize={},contentLength={}",
                    bucketName, storagePath, uploadId, partSize, contentLength);

            while (uploadedBytes < contentLength) {
                int currentPartSize = (int) Math.min(partSize, contentLength - uploadedBytes);
                byte[] partBuffer = new byte[currentPartSize];
                int offset = 0;

                while (offset < currentPartSize) {
                    int read = inputStream.read(partBuffer, offset, currentPartSize - offset);
                    if (read < 0) {
                        throw new EOFException("源文件流提前结束，expected=" + contentLength
                                + ", actual=" + uploadedBytes + offset);
                    }
                    if (read == 0) {
                        continue;
                    }
                    offset += read;
                }

                PartETag partETag = s3FileClient.uploadPart(new UploadPartRequest()
                                .withBucketName(bucketName)
                                .withKey(storagePath)
                                .withUploadId(uploadId)
                                .withPartNumber(partNumber)
                                .withInputStream(new ByteArrayInputStream(partBuffer, 0, currentPartSize))
                                .withPartSize(currentPartSize))
                        .getPartETag();

                partETags.add(partETag);
                uploadedBytes += currentPartSize;
                log.debug("[S3大文件Multipart] part上传完成 bucket={},key={},partNumber={},partSize={},uploadedBytes={}/{}",
                        bucketName, storagePath, partNumber, currentPartSize, uploadedBytes, contentLength);

                partNumber++;
                if (partNumber > MAX_PART_COUNT + 1 && uploadedBytes < contentLength) {
                    throw new IllegalStateException("Multipart分片数量超过S3最大限制: " + MAX_PART_COUNT);
                }
            }

            s3FileClient.completeMultipartUpload(new CompleteMultipartUploadRequest(
                    bucketName, storagePath, uploadId, partETags));
            log.info("[S3大文件Multipart] 完成 bucket={},key={},uploadId={},partCount={},size={}",
                    bucketName, storagePath, uploadId, partETags.size(), contentLength);
        } catch (Exception e) {
            if (StrUtil.isNotBlank(uploadId)) {
                try {
                    s3FileClient.abortMultipartUpload(new AbortMultipartUploadRequest(
                            bucketName, storagePath, uploadId));
                } catch (Exception abortException) {
                    log.error("[S3大文件Multipart] abort失败 bucket={},key={},uploadId={}",
                            bucketName, storagePath, uploadId, abortException);
                }
            }
            if (e instanceof IOException) {
                throw (IOException) e;
            }
            throw new IOException("S3大文件Multipart上传失败: " + storagePath, e);
        }
    }

    private long calculateMultipartPartSize(long contentLength) {
        long minimumPartSizeForCount = (contentLength + MAX_PART_COUNT - 1L) / MAX_PART_COUNT;
        long partSize = Math.max(DEFAULT_PART_SIZE, minimumPartSizeForCount);
        if (partSize < MIN_PART_SIZE) {
            partSize = MIN_PART_SIZE;
        }
        // S3 单个 part 最大 5GB；当前对象大小和 S3 对象上限下通常不会触发这里。
        if (partSize > 5L * 1024 * 1024 * 1024) {
            throw new IllegalArgumentException("Multipart part size超过S3最大限制: " + partSize);
        }
        return partSize;
    }

    private ObjectMetadata buildStreamUploadMetadata(Long contentLength,
                                                      String contentType,
                                                      Map<String, String> metadata) {
        ObjectMetadata objectMetadata = new ObjectMetadata();
        if (contentLength != null) {
            objectMetadata.setContentLength(contentLength);
        }
        if (StrUtil.isNotBlank(contentType)) {
            objectMetadata.setContentType(contentType);
        }
        if (!CollectionUtils.isEmpty(metadata)) {
            objectMetadata.setUserMetadata(new HashMap<>(metadata));
        }
        return objectMetadata;
    }

    @Override
    public String initiateMultipartUpload(String bucketName, String rootPath, String path, String contentType) {
        String storagePath = Tools.join(false, rootPath, path);
        InitiateMultipartUploadRequest request = new InitiateMultipartUploadRequest(bucketName, storagePath);
        if (StrUtil.isNotBlank(contentType)) {
            ObjectMetadata metadata = new ObjectMetadata();
            metadata.setContentType(contentType);
            request.setObjectMetadata(metadata);
        }
        return s3FileClient.initiateMultipartUpload(request).getUploadId();
    }

    @Override
    public PartETag uploadMultipartPart(String bucketName, String rootPath, String path,
                                        String uploadId, Integer partNumber, MultipartFile file) throws IOException {
        String storagePath = Tools.join(false, rootPath, path);
        try (InputStream inputStream = file.getInputStream()) {
            UploadPartRequest request = new UploadPartRequest()
                    .withBucketName(bucketName)
                    .withKey(storagePath)
                    .withUploadId(uploadId)
                    .withPartNumber(partNumber)
                    .withInputStream(inputStream)
                    .withPartSize(file.getSize());
            return s3FileClient.uploadPart(request).getPartETag();
        }
    }

    @Override
    public boolean checkUploadMultipartPart(String bucketName, String rootPath, String path,
                                            String uploadId, Integer partNumber) {
        return listMultipartUploadParts(bucketName, rootPath, path, uploadId).stream()
                .anyMatch(part -> part.getPartNumber() == partNumber);
    }

    @Override
    public List<PartETag> listMultipartUploadParts(String bucketName, String rootPath, String path, String uploadId) {
        String storagePath = Tools.join(false, rootPath, path);
        List<PartETag> parts = new ArrayList<>();
        ListPartsRequest request = new ListPartsRequest(bucketName, storagePath, uploadId);
        PartListing listing;
        try {
            do {
                listing = s3FileClient.listParts(request);
                parts.addAll(listing.getParts().stream()
                        .map(part -> new PartETag(part.getPartNumber(), part.getETag()))
                        .collect(Collectors.toList()));
                request.setPartNumberMarker(listing.getNextPartNumberMarker());
            } while (listing.isTruncated());
        } catch (AmazonS3Exception e) {
            log.error("[Minio 操作路径] listMultipartUploadParts：查询源数据报错 bucketName：{}，storagePath={}", bucketName, storagePath, e);
        }
        return parts;
    }

    @Override
    public String completeMultipartUpload(String bucketName, String rootPath, String path, String uploadId) {
        String storagePath = Tools.join(false, rootPath, path);
        List<PartETag> partETags = listMultipartUploadParts(bucketName, rootPath, path, uploadId);
        if (partETags.isEmpty()) {
            throw new IllegalArgumentException("没有可合并的上传分片");
        }
        partETags.sort(Comparator.comparingInt(PartETag::getPartNumber));
        s3FileClient.completeMultipartUpload(new CompleteMultipartUploadRequest(
                bucketName, storagePath, uploadId, partETags));
        return Tools.join(s3FileClientConfig.getDomain(), storagePath);
    }

    @Override
    public void abortMultipartUpload(String bucketName, String rootPath, String path, String uploadId) {
        String storagePath = Tools.join(false, rootPath, path);
        s3FileClient.abortMultipartUpload(new AbortMultipartUploadRequest(bucketName, storagePath, uploadId));
    }

    /**
     *  不把所有对象全加载内存
     */
    @Override
    public void deleteDirectory(String bucketName, String rootPath, String path) {
        // 安全校验：桶名不能为空
        if (StrUtil.isBlank(bucketName)) {
            log.error("[Minio删除操作失败] 桶名不能为空，终止执行");
            return;
        }
        // 拼接完整存储路径
        String storagePath = Tools.join(false, rootPath, path);
        log.info("[Minio 操作路径] deleteDirectory：bucketName：{}，storagePath={}", bucketName, storagePath);
        if (StrUtil.isBlank(storagePath)) {
            log.error("[Minio删除操作失败] 拼接后路径为空，禁止全桶删除，终止执行");
            return;
        }
        // 防护：禁止直接删除桶根，防止误删整桶数据
        if (StrUtil.equals(storagePath, "/")) {
            log.error("[Minio删除操作拒绝] 禁止删除桶根目录，bucket={}", bucketName);
            return;
        }

        // S3 list前缀必须尾部追加/，精准匹配目录，防止误删同名前缀文件
        String listPrefix = Tools.appendSuffix(storagePath);

        S3ObjectResult objectResult = this.whileGetListObjectsV2(bucketName, listPrefix, true);
        List<S3ObjectSummary> objectSummaries = objectResult.getObjectSummaries();
        List<String> commonPrefixes = objectResult.getCommonPrefixes();

        // 递归优先删除所有子目录
        for (String subPrefix : commonPrefixes) {
            // 子目录递归删除，rootPath传null，path直接传子前缀
            deleteDirectory(bucketName, null, subPrefix);
        }

        List<String> deleteKeyList = new ArrayList<>(objectSummaries.size());
        for (S3ObjectSummary summary : objectSummaries) {
            String sourceKey = summary.getKey();
            deleteKeyList.add(sourceKey);
        }

        if (!deleteKeyList.isEmpty()) {
            //删除文件
            batchDeleteByKey(bucketName, deleteKeyList);
            log.info("[Minio 操作路径] deleteDirectory：bucket={}, prefix={}, 本次删除key数量={}",
                    bucketName, listPrefix, deleteKeyList.size());
        } else {
            log.info("[Minio deleteDirectory] 当前目录为空，子目录已全部清理 bucket={}, prefix={}", bucketName, listPrefix);
        }

        // 兜底：主动删除当前目录占位key（xxx/），处理空文件夹遗留占位对象
        String dirPlaceholderKey = Tools.appendSuffix(storagePath);
        try {
            s3FileClient.deleteObject(bucketName, dirPlaceholderKey);
            log.debug("[Minio deleteDirectory] 兜底删除目录占位key:{}", dirPlaceholderKey);
        } catch (Exception e) {
            log.debug("[Minio deleteDirectory] 目录占位key不存在或删除失败 key={}", dirPlaceholderKey);
        }

        // 无论是否存在S3对象，空目录也要清理缓存
        this.deleteDavResourceFromCache(bucketName, rootPath, path);
        log.debug("[Minio deleteDirectory] 目录缓存已清理 bucket={}, storagePath={}", bucketName, storagePath);
    }

    @Override
    public void delete(String bucketName, String rootPath, String path) {
        String storagePath = Tools.join(false, rootPath, path);
        log.info("[Minio 操作路径] delete：bucketName：{}，storagePath={}", bucketName, storagePath);
        s3FileClient.deleteObject(bucketName, storagePath);
        this.deleteDavResourceFromCache(bucketName, rootPath, path);
    }

    @Override
    public byte[] getContent(String bucketName, String rootPath, String path) {
        String storagePath = rootPath != null ? Tools.join(false, rootPath, path) : path;
        log.info("[Minio 操作路径] getContent：bucketName：{}，storagePath={}", bucketName, storagePath);
//        String pathFile = FileTypeUtils.ensurePathStartWithSlash(path);
//        String pathFileRoot = FileTypeUtils.ensurePathStartWithSlash("");
//        String objPath = StringUtils.startsWith(pathFile, pathFileRoot) ? path : Tools.join(false, pathFileRoot, path);
        S3Object tempS3Object = s3FileClient.getObject(bucketName, storagePath);
        return IoUtil.readBytes(tempS3Object.getObjectContent());
    }

    /**
     * 托管S3Object释放的资源包装类
     * 自动关闭底层S3 http连接，防止连接泄漏
     */
    public static class S3InputStreamResource extends InputStreamResource implements AutoCloseable {
        private final S3Object s3Object;

        public S3InputStreamResource(S3Object s3Object) {
            super(s3Object.getObjectContent());
            this.s3Object = s3Object;
        }

        @Override
        public void close() throws IOException {
            if (s3Object != null) {
                s3Object.close();
            }
        }
    }

    @Override
    public void folderFileDownload(String bucketName, String zipName, String rootPath, String path, HttpServletResponse response) {
        List<Future<FileDownloadResult>> futures = new ArrayList<>();
        ExecutorService downloadExecutor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 2);

        String storagePath = Tools.join(false, rootPath, path);
        log.info("[Minio 操作路径] folderFileDownload：bucketName：{}，storagePath={}", bucketName, storagePath);
        // 获取所有文件
        List<FileRespDTO> allFiles = fileDirectList(bucketName, rootPath, path);

        response.setContentType("application/zip");
        String zipFileName = zipName + ".zip";
        ContentDisposition contentDisposition = ContentDisposition.attachment()
                .filename(zipFileName, StandardCharsets.UTF_8)
                .build();
        response.setHeader(HttpHeaders.CONTENT_DISPOSITION, contentDisposition.toString());

        for (FileRespDTO file : allFiles) {
            if (file.getFolder()) {
                continue;
            }
            futures.add(downloadExecutor.submit(() -> {
                try {
                    S3Object tempS3Object = s3FileClient.getObject(bucketName, file.getOriginalPath());
                    return new FileDownloadResult(file, IoUtil.readBytes(tempS3Object.getObjectContent()), null);
                } catch (Exception e) {
                    log.error("下载文件失败: {}，error: {}", file.getOriginalPath(), e.getMessage());
                    return new FileDownloadResult(file, null, e);
                }
            }));
        }

        String removeStoragePath = Tools.removeSuffix(storagePath);
        removeStoragePath = removeStoragePath.substring(0, removeStoragePath.lastIndexOf("/"));
        // 顺序写入zip
        try (ZipOutputStream zos = new ZipOutputStream(response.getOutputStream())) {
            byte[] buffer = new byte[8192];
            for (Future<FileDownloadResult> future : futures) {
                FileDownloadResult result;
                try {
                    result = future.get();
                } catch (Exception e) {
                    log.error("获取下载结果失败: {}", e.getMessage());
                    continue;
                }
                if (result.getContent() == null) {
                    continue;
                }
                String fullFilePath = result.getFile().getOriginalPath();
                String zipEntryName = fullFilePath;
                if (StrUtil.isNotEmpty(removeStoragePath) && fullFilePath.startsWith(removeStoragePath)) {
                    zipEntryName = fullFilePath.substring(removeStoragePath.length());
                }
                zipEntryName = Tools.removePrefix(zipEntryName);
                if (StrUtil.isEmpty(zipEntryName)) {
                    log.warn("文件路径处理异常，跳过: {}", fullFilePath);
                    continue;
                }
                zos.putNextEntry(new ZipEntry(zipEntryName));
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
            // 修复：shutdown + awaitTermination，等待下载线程结束
            downloadExecutor.shutdown();
            try {
                if (!downloadExecutor.awaitTermination(8, TimeUnit.SECONDS)) {
                    downloadExecutor.shutdownNow();
                }
            } catch (InterruptedException ie) {
                downloadExecutor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }

    @Override
    public void deleteDavResourceFromCache(String bucketName, String path) {
        this.deleteDavResourceFromCache(bucketName, path, "/");
    }

    @Override
    public void deleteDavResourceFromCache(String bucketName, String rootPath, String path) {
        CompletableFuture.runAsync(() -> {
            try {
                // 参数防御
                if (StrUtil.isBlank(bucketName) || StrUtil.isBlank(path)) {
                    log.warn("[deleteDavResourceFromCache] 参数为空，跳过缓存失效 bucketName={},path={}", bucketName, path);
                    return;
                }

                // 删除自身缓存
                String fullPath = Tools.join(false, rootPath, path);
                String key = "s3:list:" + bucketName + ":" + fullPath;
                stringRedisTemplate.delete(key);
                log.info("[Minio 操作路径] delete Cache：key={}", key);

                // 计算父路径，先移除末尾斜杠，再找最后一个斜杠下标
                String purePath = Tools.removeSuffix(path);
                int lastSlashIndex = purePath.lastIndexOf("/");
                // lastSlashIndex > 0：存在有效父目录；等于0代表根下一级，无业务父目录；-1无斜杠
                if (lastSlashIndex > 0) {
                    String parentRelativePath = purePath.substring(0, lastSlashIndex);
                    String parentFullPath = Tools.join(false, rootPath, parentRelativePath);
                    String key2 = "s3:list:" + bucketName + ":" + parentFullPath;
                    stringRedisTemplate.delete(key2);
                    log.info("[Minio 操作路径] delete Cache：key={}", key2);
                } else {
                    log.debug("[deleteDavResourceFromCache] 当前已是顶层路径，无需失效父缓存 path={}", path);
                }
            } catch (Exception e) {
                log.error("[deleteDavResourceFromCache] 异步缓存删除异常 bucket={},path={}", bucketName, path, e);
            }
        });
    }

    @Override
    public void createDirectory(String bucketName, String rootPath, String path) {
        this.createDirectory(bucketName, rootPath, path, null);
    }

    @Override
    public void createDirectory(String bucketName, String rootPath, String path, Map<String, String> userMetadata) {
        String storagePath = Tools.join(rootPath, path);
        log.info("[Minio 操作路径] createDirectory：bucketName：{}，storagePath={}", bucketName, storagePath);
        // 元数据，主要用于设置文件类型
        ObjectMetadata objectMetadata = new ObjectMetadata();
        // 如果不设置，会有 “ No content length specified for stream data” 警告日志
        objectMetadata.setContentLength(0L);
        // 设置内容类型，表明这是一个目录
        objectMetadata.setContentType("application/x-directory");
        if (userMetadata != null) {
            objectMetadata.setUserMetadata(userMetadata);
        }
        // 构建请求对象
        PutObjectRequest request = new PutObjectRequest(
                bucketName,
                storagePath + "/",
                new ByteArrayInputStream(new byte[0]),
                objectMetadata
        );
        // 执行上传
        s3FileClient.putObject(request);
        // 2. 清除缓存
        this.deleteDavResourceFromCache(bucketName, rootPath, path);
    }

    @Override
    public void copyDirectory(String bucketName, String sourcePath, String targetPath) {
        final String finalSourcePath = Tools.removePrefix(sourcePath);
        final String finalTargetPath = Tools.removePrefix(sourcePath);
        // 获取源文件列表
        List<S3ObjectSummary> s3ObjectSummaries = getS3ObjectSummaries(bucketName, finalSourcePath);
        for (S3ObjectSummary objectSummary : s3ObjectSummaries) {
            String sourceKey = objectSummary.getKey();
            // 核心逻辑：替换前缀
            // 将 "1/4277/文件夹1/xxx/..." 替换为 "1/4277/文件夹2/xxx/..."
            String destinationKey = sourceKey.replace(finalSourcePath, finalTargetPath);
            // 同 Bucket 复制
            s3FileClient.copyObject(
                    bucketName,
                    sourceKey,
                    bucketName,
                    destinationKey
            );
        }
    }

    /**
     * 复制文件夹
     *
     * @param sourceBucketName 原桶名称
     * @param sourceFolderPath 【文件夹】源路径 案例：/folder1/test1
     * @param targetBucketName 目标桶名称
     * @param targetFolderPath 【文件夹】目标路径 案例：/folder2/test3
     */
    @Override
    public void copyDirectory(String sourceBucketName, String sourceFolderPath, String targetBucketName, String targetFolderPath) {
        log.info("[Minio 操作路径] copyDirectory：sourceBucket={}, sourceFolderPath={}, targetBucket={}, targetFolderPath}",
                sourceBucketName, sourceFolderPath, targetBucketName, targetFolderPath);

        String sourceFolderPathSuffix = Tools.appendSuffix(Tools.removePrefix(sourceFolderPath));
        String targetFolderPathSuffix = Tools.appendSuffix(Tools.removePrefix(targetFolderPath));

        // ==========新增防护：同桶并且源路径等于目标路径，直接返回，禁止拷贝到自身==========
        if (Objects.equals(sourceBucketName, targetBucketName)
                && Objects.equals(sourceFolderPathSuffix, targetFolderPathSuffix)) {
            log.warn("[Minio copyDirectory] source path equals target path, skip copy self. path={}", sourceFolderPathSuffix);
            return;
        }

        S3ObjectResult objectResult = this.whileGetListObjectsV2(sourceBucketName, sourceFolderPathSuffix, false);
        List<S3ObjectSummary> objectSummaries = objectResult.getObjectSummaries();
        List<String> commonPrefixes = objectResult.getCommonPrefixes();

        for (S3ObjectSummary objectSummary : objectSummaries) {
            String sourceKey = objectSummary.getKey();
            if (!sourceKey.startsWith(sourceFolderPathSuffix)) {
                continue;
            }
            if (sourceKey.endsWith("/")) {
                log.info("[Minio copyDirectory] skip marker key={},size={}", sourceKey, objectSummary.getSize());
                continue;
            }

            String destinationKey = safeReplaceS3Prefix(sourceKey, sourceFolderPathSuffix, targetFolderPathSuffix);
            log.info("[Minio 操作路径] copyDirectory‑objectSummaries：sourceKey={},targetBucket={},destinationKey={}",
                    sourceKey, targetBucketName, destinationKey);
            try {
                // 替换原来直接 copyObject，内部自动区分大文件分片拷贝
                copySingleObject(sourceBucketName, sourceKey, targetBucketName, destinationKey);
            } catch (Exception e) {
                log.error("[Minio copyDirectory] copy failed. sourceBucket={},sourceKey={},destBucket={},destKey={}",
                        sourceBucketName, sourceKey, targetBucketName, destinationKey, e);
                e.printStackTrace();
            }
        }

        for (String subDirPrefix : commonPrefixes) {
            String subTargetPrefix = safeReplaceS3Prefix(subDirPrefix, sourceFolderPathSuffix, targetFolderPathSuffix);
            copyDirectory(sourceBucketName, subDirPrefix, targetBucketName, subTargetPrefix);
        }

        this.deleteDavResourceFromCache(sourceBucketName, sourceFolderPathSuffix);
        this.deleteDavResourceFromCache(targetBucketName, targetFolderPathSuffix);
    }

    /**
     * 拷贝单个S3对象，自动区分小文件直拷 / 大文件分片拷贝
     * <p>服务端拷贝，数据不经过应用服务器网络；
     * <ol>
     * <li>文件 ≤5GB：使用CopyObject直接拷贝</li>
     * <li>文件 >5GB：使用Multipart Copy分片拷贝，自动计算分片大小，分片数不超过10000</li>
     * </ol>
     * <p><b>风险说明：</b>分片拷贝中途异常会主动abort未完成分片任务，避免存储侧遗留垃圾碎片；
     * 注意元数据默认不会自动复制，业务需要请手动设置initReq.setObjectMetadata(sourceMeta)
     * </p>
     *
     * @param sourceBucket 源存储桶【非空】
     * @param sourceKey    源对象key【非空】
     * @param targetBucket 目标存储桶【非空】
     * @param targetKey    目标对象key【非空】
     * @throws IllegalArgumentException bucket/key入参为空抛出
     * @throws AmazonS3Exception        S3服务返回错误（对象不存在、权限不足等）
     */
    private void copySingleObject(@Nonnull String sourceBucket,
                                  @Nonnull String sourceKey,
                                  @Nonnull String targetBucket,
                                  @Nonnull String targetKey) {
        // 卫语句：入参校验，阿里云规范，方法最开始做参数校验
        if (StringUtils.isAnyBlank(sourceBucket, sourceKey, targetBucket, targetKey)) {
            log.error("[copySingleObject] 非法入参 sourceBucket={},sourceKey={},targetBucket={},targetKey={}",
                    sourceBucket, sourceKey, targetBucket, targetKey);
            throw new IllegalArgumentException("bucket与key不能为空");
        }

        String uploadId = null;
        try {
            // 获取源对象元信息
            ObjectMetadata sourceMeta = s3FileClient.getObjectMetadata(sourceBucket, sourceKey);
            long fileSize = sourceMeta.getContentLength();
            log.info("[copySingleObject] 开始拷贝 sourceBucket={},sourceKey={},targetBucket={},targetKey={},fileSize={}",
                    sourceBucket, sourceKey, targetBucket, targetKey, fileSize);

            // 0字节空文件直接返回，无需拷贝
            if (fileSize == 0L) {
                log.info("[copySingleObject] 源对象为空文件，跳过拷贝 sourceBucket={},sourceKey={}", sourceBucket, sourceKey);
                return;
            }

            // ≤5GB 使用直接拷贝
            if (fileSize <= MAX_DIRECT_COPY) {
                log.info("[copySingleObject] 使用直接拷贝模式 fileSize={} ≤5GB", fileSize);
                CopyObjectRequest objectRequest = new CopyObjectRequest(sourceBucket, sourceKey, targetBucket, targetKey);
                s3FileClient.copyObject(objectRequest);
                log.info("[copySingleObject] 直接拷贝成功 sourceKey={},targetKey={}", sourceKey, targetKey);
                return;
            }

            // ----------------------大于5GB，Multipart Copy 服务端分片拷贝----------------------
            log.info("[copySingleObject] 文件大于5GB，启用分片拷贝 fileSize={},sourceKey={}", fileSize, sourceKey);

            long partSize = DEFAULT_PART_SIZE;
            // 如果预估分片数超过上限，自动放大分片大小
            if (fileSize / partSize > MAX_PART_COUNT) {
                partSize = (fileSize / MAX_PART_COUNT) + PART_SIZE_OFFSET;
                log.info("[copySingleObject] 文件超大自动调整分片大小 originPartSize={},adjustPartSize={}",
                        DEFAULT_PART_SIZE, partSize);
            }
            // S3强制约束：分片不能小于5MB
            if (partSize < MIN_PART_SIZE) {
                partSize = MIN_PART_SIZE;
                log.warn("[copySingleObject] 分片小于最小限制，强制修正为5MB");
            }

            // 初始化分片上传
            InitiateMultipartUploadRequest initReq = new InitiateMultipartUploadRequest(targetBucket, targetKey);
            // 业务需要复制源对象元数据打开下面注释
            // initReq.setObjectMetadata(sourceMeta);
            InitiateMultipartUploadResult initResp = s3FileClient.initiateMultipartUpload(initReq);
            uploadId = initResp.getUploadId();
            log.info("[copySingleObject] 初始化分片任务成功 uploadId={},partSize={}", uploadId, partSize);

            List<PartETag> partETagList = new ArrayList<>();
            long offset = 0L;
            int partNumber = 1;

            while (offset < fileSize) {
                long end = Math.min(offset + partSize - 1, fileSize - 1);
                CopyPartRequest copyPartReq = new CopyPartRequest()
                        .withSourceBucketName(sourceBucket)
                        .withSourceKey(sourceKey)
                        .withDestinationBucketName(targetBucket)
                        .withDestinationKey(targetKey)
                        .withUploadId(uploadId)
                        .withPartNumber(partNumber)
                        .withFirstByte(offset)
                        .withLastByte(end);

                log.trace("[copySingleObject] 拷贝分片 uploadId={},partNum={},offset={},end={}",
                        uploadId, partNumber, offset, end);
                CopyPartResult partResult = s3FileClient.copyPart(copyPartReq);
                partETagList.add(new PartETag(partNumber, partResult.getETag()));
                log.trace("[copySingleObject] 分片完成 uploadId={},partNum={},eTag={}",
                        uploadId, partNumber, partResult.getETag());

                offset = end + 1;
                partNumber++;
            }

            log.info("[copySingleObject] 全部分片拷贝完成 uploadId={},totalPart={}", uploadId, partETagList.size());
            CompleteMultipartUploadRequest completeReq = new CompleteMultipartUploadRequest(targetBucket, targetKey, uploadId, partETagList);
            s3FileClient.completeMultipartUpload(completeReq);
            log.info("[copySingleObject] 分片拷贝合并成功 sourceKey={},targetKey={},uploadId={}",
                    sourceKey, targetKey, uploadId);

        } catch (Exception e) {
            log.error("[copySingleObject] 对象拷贝异常 sourceBucket={},sourceKey={},targetBucket={},targetKey={}",
                    sourceBucket, sourceKey, targetBucket, targetKey, e);
            // 分片任务初始化成功，发生异常必须中止分片，防止S3遗留未完成multipart垃圾
            if (StringUtils.isNotBlank(uploadId)) {
                abortMultipartUpload(targetBucket, targetKey, uploadId);
            }
            // 原样抛出，由上层处理
            throw e;
        }
    }


    /**
     * 从sourceBucketName目录移动文件到targetBucketName目录下
     *
     * @param sourceBucketName 原桶名称
     * @param sourceFolderPath 【文件夹】源路径 案例：/folder1/test1
     * @param targetBucketName 目标桶名称
     * @param targetFolderPath 【文件夹】目标路径 案例：/folder2/test3
     */
    /**
     * 从sourceBucketName目录移动文件到targetBucketName目录下
     *
     * @param sourceBucketName 原桶名称
     * @param sourceFolderPath 【文件夹】源路径 案例：/folder1/test1
     * @param targetBucketName 目标桶名称
     * @param targetFolderPath 【文件夹】目标路径 案例：/folder2/test3
     */
    @Override
    public void moveDirectory(String sourceBucketName, String sourceFolderPath, String targetBucketName, String targetFolderPath) {
        // ========= 入参安全校验 =========
        if (StrUtil.isBlank(sourceBucketName) || StrUtil.isBlank(sourceFolderPath)
                || StrUtil.isBlank(targetBucketName) || StrUtil.isBlank(targetFolderPath)) {
            log.error("[Minio moveDirectory] 参数不能为空 sourceBucket={},sourcePath={},targetBucket={},targetPath={}",
                    sourceBucketName, sourceFolderPath, targetBucketName, targetFolderPath);
            throw new IllegalArgumentException("moveDirectory入参不能为空");
        }

        log.info("[Minio 操作路径] moveDirectory：sourceBucketName：{}，sourceFolderPath={}，targetBucketName={},targetFolderPath={}",
                sourceBucketName, sourceFolderPath, targetBucketName, targetFolderPath);

        String sourceFolderPathSuffix = Tools.appendSuffix(Tools.removePrefix(sourceFolderPath));
        String targetFolderPathSuffix = Tools.appendSuffix(Tools.removePrefix(targetFolderPath));

        // 【新增防护】禁止移动到自身，同桶且路径相等直接返回
        if (Objects.equals(sourceBucketName, targetBucketName)
                && Objects.equals(sourceFolderPathSuffix, targetFolderPathSuffix)) {
            log.warn("[Minio moveDirectory] source path equals target path, skip move self. path={}", sourceFolderPathSuffix);
            return;
        }

        S3ObjectResult objectResult = this.whileGetListObjectsV2(sourceBucketName, sourceFolderPathSuffix, false);
        List<S3ObjectSummary> objectSummaries = objectResult.getObjectSummaries();
        List<String> commonPrefixes = objectResult.getCommonPrefixes();

        List<String> deleteKeys = new ArrayList<>(objectSummaries.size());
        // 复制当前层级全部文件
        for (S3ObjectSummary objectSummary : objectSummaries) {
            String sourceKey = objectSummary.getKey();
            if (!sourceKey.startsWith(sourceFolderPathSuffix)) {
                continue;
            }
            // 跳过S3目录marker占位对象，不拷贝、不删除marker
            if (objectSummary.getSize() == 0 && sourceKey.endsWith("/")) {
                log.info("[Minio moveDirectory] skip folder marker object, sourceKey={}", sourceKey);
                continue;
            }
            String destinationKey = safeReplaceS3Prefix(sourceKey, sourceFolderPathSuffix, targetFolderPathSuffix);
            log.info("[Minio moveDirectory] copy sourceKey={},destKey={}", sourceKey, destinationKey);

            try {
                // 替换原有copyObject，自动处理>5GB分片拷贝
                copySingleObject(sourceBucketName, sourceKey, targetBucketName, destinationKey);
            } catch (AmazonS3Exception e) {
                // 识别MinIO磁盘满507
                if ("XMinioStorageFull".equals(e.getErrorCode()) || 507 == e.getStatusCode()) {
                    log.error("[Minio moveDirectory] 存储后端磁盘空间不足 507 XMinioStorageFull，move中断", e);
                    throw new RuntimeException("存储后端磁盘空间不足，请清理存储后重试", e);
                }
                log.error("[Minio moveDirectory] copy failed sourceKey={},destKey={}", sourceKey, destinationKey, e);
                throw e;
            } catch (Exception e) {
                log.error("[Minio moveDirectory] copy exception sourceKey={},destKey={}", sourceKey, destinationKey, e);
                throw new RuntimeException("[Minio moveDirectory] copy exception", e);
            }
            // 拷贝成功之后才加入待删除列表
            deleteKeys.add(sourceKey);
        }

        // 递归处理所有子目录（重要！多层子目录移动）
        for (String subDirPrefix : commonPrefixes) {
            String subTargetPrefix = safeReplaceS3Prefix(subDirPrefix, sourceFolderPathSuffix, targetFolderPathSuffix);
            moveDirectory(sourceBucketName, subDirPrefix, targetBucketName, subTargetPrefix);
        }

        // 当前层级拷贝全部成功，批量删除源文件
        if (!deleteKeys.isEmpty()) {
            //【关键点2】分片批量删除，每批严格不超过1000，彻底规避MalformedXML
            batchDeleteByKey(sourceBucketName, deleteKeys);
            log.info("[Minio moveDirectory] batch delete source count={}", deleteKeys.size());
        }
        // 清理WebDAV缓存
        this.deleteDavResourceFromCache(sourceBucketName, sourceFolderPathSuffix);
        this.deleteDavResourceFromCache(targetBucketName, targetFolderPathSuffix);
    }

    /**
     * 安全替换S3对象key前缀
     *
     * @param key          对象完整key
     * @param sourcePrefix 源前缀（必须带尾部/）
     * @param targetPrefix 目标前缀（必须带尾部/）
     * @return 新key
     */
    private String safeReplaceS3Prefix(String key, String sourcePrefix, String targetPrefix) {
        if (!key.startsWith(sourcePrefix)) {
            throw new IllegalArgumentException("key不匹配源前缀, key=" + key + ",prefix=" + sourcePrefix);
        }
        return targetPrefix + key.substring(sourcePrefix.length());
    }

    /**
     * 将源存储桶目录下所有文件打包为zip，分片上传至目标桶
     * <p>执行流程：
     * <ol>
     * <li>参数校验、路径标准化，防止源输出路径冲突</li>
     * <li>本地磁盘预校验，创建完整zip临时磁盘文件</li>
     * <li>分页迭代拉取S3源目录对象，流式下载写入本地zip临时文件</li>
     * <li>全部文件写入完成关闭ZipOutputStream，保证zip中央目录完整</li>
     * <li>读取本地完整zip文件，执行S3 Multipart分片上传</li>
     * <li>上传成功返回；异常中止分片上传，清理本地临时文件</li>
     * </ol>
     *
     * @param sourceBucketName 源存储桶名称【非空】
     * @param sourceFolderPath 源目录路径
     * @param targetBucketName 目标存储桶名称【非空】
     * @param targetZipPath    输出zip对象完整key
     * @param skipFileType     需要跳过的文件后缀黑名单；传null代表不跳过任何文件；例 [".tmp",".log"]
     * @throws IllegalArgumentException 入参非法、源目录为空、源输出路径冲突抛出
     * @throws IOException              本地磁盘IO异常，磁盘空间不足
     * @throws RuntimeException         S3读取、分片上传业务异常；内部会自动abort未完成分片
     */
    @Override
    public void copyDirectoryToZip(@Nonnull String sourceBucketName,
                                   @Nonnull String sourceFolderPath,
                                   @Nonnull String targetBucketName,
                                   @Nonnull String targetZipPath,
                                   @Nullable List<String> skipFileType) {
        log.info("[copyDirectoryToZip] 入参 sourceBucketName={},sourceFolderPath={},targetBucketName={},targetZipPath={},skipFileTypeSize={}",
                sourceBucketName, sourceFolderPath, targetBucketName, targetZipPath,
                skipFileType == null ? 0 : skipFileType.size());

        // ===================== 参数卫语句校验 =====================
        if (StrUtil.isBlank(sourceBucketName)) {
            throw new IllegalArgumentException("源存储桶名称不能为空");
        }
        if (StrUtil.isBlank(targetBucketName)) {
            throw new IllegalArgumentException("目标存储桶名称不能为空");
        }
        if (StrUtil.isBlank(targetZipPath)) {
            throw new IllegalArgumentException("目标zip路径不能为空");
        }

        // 源目录路径标准化，前后去斜杠+结尾补斜杠
        String sourcePrefix = Tools.removePrefix(Tools.appendSuffix(sourceFolderPath));
        if (StrUtil.isBlank(sourcePrefix)) {
            log.error("[copyDirectoryToZip] 源目录路径为空，禁止全桶扫描 sourceFolderPath={}", sourceFolderPath);
            throw new IllegalArgumentException("源目录路径不能为空，禁止全桶扫描");
        }

        // 目标zip key标准化：剔除开头所有"/"，避免S3 key首字符带斜杠产生空虚拟目录
        String targetZipKeyNormalized = targetZipPath;
        while (targetZipKeyNormalized.startsWith("/")) {
            targetZipKeyNormalized = targetZipKeyNormalized.substring(1);
        }
        if (StrUtil.isBlank(targetZipKeyNormalized)) {
            throw new IllegalArgumentException("标准化后目标zip key为空");
        }
        log.info("[copyDirectoryToZip] 标准化后最终S3 zip key={}", targetZipKeyNormalized);

        // 防护：源目录不能和输出zip路径冲突，防止打包自身
        if (Objects.equals(sourceBucketName, targetBucketName) && sourcePrefix.equals(targetZipKeyNormalized)) {
            throw new IllegalArgumentException("源目录与输出zip路径冲突，禁止打包自身");
        }

        String uploadId = null;
        File tempZipFile = null;

        try {
            // 创建本地完整zip临时磁盘文件，所有zip内容先落磁盘，规避JVM OOM，保证zip归档格式合法
            tempZipFile = FileUtil.createTempFile("s3_full_zip_", ".tmp", null, true);
            log.info("[copyDirectoryToZip] 创建本地zip临时文件 absolutePath={}", tempZipFile.getAbsolutePath());

            // 预校验本地磁盘可用空间
            Path tempFilePath = tempZipFile.toPath();
            FileStore fileStore = Files.getFileStore(tempFilePath);
            long availableSpace = fileStore.getUsableSpace();
            long minRequireDiskSpace = (long) (DEFAULT_PART_SIZE * DISK_SPACE_RESERVE_RATIO);
            if (availableSpace < minRequireDiskSpace) {
                log.error("[copyDirectoryToZip] 本地磁盘空间不足 availableSpace={}, minRequireDiskSpace={}", availableSpace, minRequireDiskSpace);
                throw new IOException("本地临时磁盘空间不足，无法执行zip打包，请检查磁盘");
            }

            boolean hasValidFile = false;

            // -------------------第一步：流式下载S3全部对象，写完整本地zip临时文件-------------------
            try (FileOutputStream fos = new FileOutputStream(tempZipFile);
                 ZipOutputStream zos = new ZipOutputStream(fos, StandardCharsets.UTF_8)) {

                // 分页迭代拉取S3对象，避免百万小文件一次性加载到内存导致OOM
                Iterator<S3ObjectSummary> objectIterator = collectAllS3ObjectIterator(sourceBucketName, sourcePrefix);

                while (objectIterator.hasNext()) {
                    S3ObjectSummary summary = objectIterator.next();
                    String sourceKey = summary.getKey();

                    // 1.跳过MinIO虚拟目录占位空对象：size=0并且key以斜杠结尾
                    if (summary.getSize() == 0 && sourceKey.endsWith("/")) {
                        log.debug("[copyDirectoryToZip] 跳过虚拟目录标记文件 key={}", sourceKey);
                        continue;
                    }

                    // 2.跳过业务空文件（size等于0的普通文件，不打入压缩包）
                    if (summary.getSize() == 0) {
                        log.debug("[copyDirectoryToZip] 跳过空文件 sourceKey={}", sourceKey);
                        continue;
                    }

                    // 3.按后缀黑名单跳过指定文件
                    boolean skipFile = false;
                    if (!CollectionUtils.isEmpty(skipFileType)) {
                        String fileName = sourceKey.substring(sourceKey.lastIndexOf("/") + 1);
                        String lowerName = fileName.toLowerCase(Locale.ROOT);
                        for (String suffix : skipFileType) {
                            if (StrUtil.isBlank(suffix)) {
                                continue;
                            }
                            String lowerSuffix = suffix.toLowerCase(Locale.ROOT);
                            if (lowerName.endsWith(lowerSuffix)) {
                                log.info("[copyDirectoryToZip] 按规则跳过文件 sourceKey={},matchSuffix={}", sourceKey, suffix);
                                skipFile = true;
                                break;
                            }
                        }
                    }
                    if (skipFile) {
                        continue;
                    }

                    hasValidFile = true;
                    // zip内entry相对路径，剥离源目录前缀，自动保留子目录层级
                    String entryName = sourceKey.substring(sourcePrefix.length());
                    ZipEntry entry = new ZipEntry(entryName);
                    entry.setTime(System.currentTimeMillis());
                    zos.putNextEntry(entry);

                    // S3对象流式下载写入zip输出流，支持单个源超大文件，数据走磁盘不占用堆内存
                    try (S3Object s3Obj = s3FileClient.getObject(sourceBucketName, sourceKey);
                         InputStream in = s3Obj.getObjectContent()) {

                        byte[] buffer = new byte[IO_BUFFER_SIZE];
                        int readLen;
                        while ((readLen = in.read(buffer)) != -1) {
                            zos.write(buffer, 0, readLen);
                            // 禁止循环调用zos.flush，会破坏zip元数据，严重损耗IO性能
                        }
                    } catch (AmazonS3Exception e) {
                        // MinIO磁盘已满507错误码特殊识别
                        if ("XMinioStorageFull".equals(e.getErrorCode()) || 507 == e.getStatusCode()) {
                            log.error("[copyDirectoryToZip] MinIO后端存储磁盘已满507 sourceKey={}", sourceKey, e);
                            throw new RuntimeException("存储后端磁盘空间不足，请清理存储后重试,sourceKey:" + sourceKey, e);
                        }
                        log.error("[copyDirectoryToZip] 读取源S3对象异常 sourceKey={}", sourceKey, e);
                        throw new RuntimeException("文件打包读取异常,sourceKey:" + sourceKey, e);
                    } catch (Exception e) {
                        log.error("[copyDirectoryToZip] 写入zip流发生异常 sourceKey={}", sourceKey, e);
                        throw new RuntimeException("文件打包读取异常,sourceKey:" + sourceKey, e);
                    }
                    zos.closeEntry();
                    log.debug("[copyDirectoryToZip] 已完成打包 entryName={},sourceFileSize={}", entryName, summary.getSize());
                }

                // 过滤完成后没有有效业务文件（空文件夹 / 全部是空文件 /全部被过滤）：直接退出，不上传zip
                if (!hasValidFile) {
                    log.warn("[copyDirectoryToZip] 无有效可打包文件(空文件夹/全部为空文件/被过滤)，不生成zip sourceBucketName={},sourcePrefix={}", sourceBucketName, sourcePrefix);
                    return;
                }
            }
            // try‑with‑resources自动关闭fos、zos；ZipOutputStream关闭才会写入zip末尾中央目录，保证归档合法
            long totalZipFileSize = tempZipFile.length();
            log.info("[copyDirectoryToZip] 本地zip文件生成完成 totalZipFileSize={} byte", totalZipFileSize);

            // 业务上限校验：不能超过S3最大分片总容量
            final long maxSupportTotalSize = DEFAULT_PART_SIZE * MAX_PART_COUNT;
            if (totalZipFileSize > maxSupportTotalSize) {
                log.error("[copyDirectoryToZip] zip文件超出S3分片上限 totalZipFileSize={},maxSupportTotalSize={}", totalZipFileSize, maxSupportTotalSize);
                throw new IllegalStateException("待生成ZIP体积过大，超过S3分片最大支持，请拆分源目录或者调高分片大小");
            }

            ObjectMetadata zipMeta = new ObjectMetadata();
            zipMeta.setContentType("application/zip");
            zipMeta.setContentLength(totalZipFileSize);

            // -------------------第二步：判断大小，小文件直接putObject；大文件走Multipart分片上传-------------------
            if (totalZipFileSize <= DEFAULT_PART_SIZE) {
                // 小文件：单请求上传，不走分片
                log.info("[copyDirectoryToZip] zip为小文件，使用putObject直接上传 size={}", totalZipFileSize);
                PutObjectRequest putReq = new PutObjectRequest(targetBucketName, targetZipKeyNormalized, tempZipFile);
                putReq.setMetadata(zipMeta);
                s3FileClient.putObject(putReq);
            } else {
                // 大文件：Multipart分片上传
                log.info("[copyDirectoryToZip] zip为大文件，执行multipart分片上传 size={}", totalZipFileSize);
                InitiateMultipartUploadRequest initReq = new InitiateMultipartUploadRequest(targetBucketName, targetZipKeyNormalized);
                initReq.setObjectMetadata(zipMeta);
                InitiateMultipartUploadResult uploadInit = s3FileClient.initiateMultipartUpload(initReq);
                uploadId = uploadInit.getUploadId();

                List<PartETag> partETagList = new ArrayList<>();
                int partNumber = 1;
                try (RandomAccessFile raf = new RandomAccessFile(tempZipFile, "r")) {
                    long fileRemaining = totalZipFileSize;
                    while (fileRemaining > 0) {
                        long currentPartSize = Math.min(DEFAULT_PART_SIZE, fileRemaining);
                        PartETag partETag = uploadSinglePartFromFile(targetBucketName, targetZipKeyNormalized, uploadId,
                                partNumber++, tempZipFile, raf, totalZipFileSize - fileRemaining, currentPartSize);
                        partETagList.add(partETag);
                        fileRemaining -= currentPartSize;

                        if (partNumber > MAX_PART_COUNT) {
                            throw new IllegalStateException("分片数量超出S3最大限制");
                        }
                    }
                }
                log.info("[copyDirectoryToZip] 所有分片上传完成 partTotalCount={}", partETagList.size());
                CompleteMultipartUploadRequest completeReq = new CompleteMultipartUploadRequest(
                        targetBucketName, targetZipKeyNormalized, uploadId, partETagList
                );
                s3FileClient.completeMultipartUpload(completeReq);
            }

            // head校验：确认zip对象真实存在，避免分片合并返回成功实际对象未生成
            try {
                ObjectMetadata headMeta = s3FileClient.getObjectMetadata(targetBucketName, targetZipKeyNormalized);
                log.info("[copyDirectoryToZip] head校验成功，zip对象真实存在，size={}, contentType={}",
                        headMeta.getContentLength(), headMeta.getContentType());
            } catch (AmazonS3Exception headEx) {
                if ("NoSuchKey".equals(headEx.getErrorCode())) {
                    log.error("[copyDirectoryToZip] 上传完成，但目标zip对象不存在 bucket={},key={}",
                            targetBucketName, targetZipKeyNormalized);
                    throw new RuntimeException("上传完成，但ZIP对象未生成", headEx);
                }
                throw headEx;
            }

            log.info("[copyDirectoryToZip] 打包上传成功 targetZipKey={},totalZipFileSize={}",
                    targetZipKeyNormalized, totalZipFileSize);

        } catch (Exception e) {
            log.error("[copyDirectoryToZip] 打包任务全局异常 sourceBucketName={},sourceFolderPath={},targetZipPath={}",
                    sourceBucketName, sourceFolderPath, targetZipPath, e);
            // 只有分片模式才有uploadId，小文件putObject无uploadId，不需要abort
            if (StrUtil.isNotBlank(uploadId)) {
                abortMultipartUpload(targetBucketName, targetZipKeyNormalized, uploadId);
            }
            throw new RuntimeException("S3目录打包压缩上传失败", e);
        } finally {
            // 强制清理本地临时磁盘文件；JVM被kill场景不会进入finally，依赖外部定时任务清理临时目录
            if (tempZipFile != null && tempZipFile.exists()) {
                boolean deleteResult = tempZipFile.delete();
                log.info("[copyDirectoryToZip] 清理本地临时文件 path={},deleteResult={}", tempZipFile.getAbsolutePath(), deleteResult);
            }
        }
    }

    /**
     * 从本地文件指定偏移位置读取一段数据，生成一个分片上传到S3
     * <p>会内部创建极小临时分片文件，上传结束自动删除
     *
     * @param bucket    目标桶
     * @param key       对象key
     * @param uploadId  分片任务id
     * @param partNum   分片序号，从1开始
     * @param localFile 源完整zip本地文件
     * @param raf       已打开随机访问文件句柄
     * @param offset    文件读取起始偏移字节
     * @param partSize  当前分片字节大小
     * @return PartETag 分片etag对象，用于最后complete合并
     * @throws IOException IO读写异常
     */
    private PartETag uploadSinglePartFromFile(String bucket, String key, String uploadId,
                                              int partNum, File localFile, RandomAccessFile raf,
                                              long offset, long partSize) throws IOException {
        raf.seek(offset);
        byte[] buffer = new byte[IO_BUFFER_SIZE];

        // 直接包装输入流，不再生成 partTempFile 磁盘临时文件
        InputStream partInputStream = new InputStream() {
            private long remain = partSize;

            @Override
            public int read() throws IOException {
                if (remain <= 0) {
                    return -1;
                }
                int b = raf.read();
                if (b != -1) {
                    remain--;
                }
                return b;
            }

            @Override
            public int read(byte[] b, int off, int len) throws IOException {
                if (remain <= 0) {
                    return -1;
                }
                int realRead = raf.read(b, off, (int) Math.min(len, remain));
                if (realRead != -1) {
                    remain -= realRead;
                }
                return realRead;
            }
        };

        UploadPartRequest uploadPartReq = new UploadPartRequest()
                .withBucketName(bucket)
                .withKey(key)
                .withUploadId(uploadId)
                .withPartNumber(partNum)
                .withInputStream(partInputStream)
                .withPartSize(partSize);

        UploadPartResult uploadPartResult = s3FileClient.uploadPart(uploadPartReq);
        return uploadPartResult.getPartETag();
    }

    /**
     * 分页迭代拉取S3对象列表，避免一次性加载全部对象入内存，防止百万小文件OOM
     * <p>内部调用 listObjectsV2 分页，使用continuationToken循环获取，返回迭代器；
     * 自动过滤S3目录占位对象(key以/结尾)，只返回真实文件对象
     *
     * @param bucket 桶名【非空】
     * @param prefix 对象前缀（目录前缀，末尾建议带斜杠）
     * @return 对象迭代器，懒加载分页数据
     * @throws IllegalArgumentException bucket为空抛出
     */
    private Iterator<S3ObjectSummary> collectAllS3ObjectIterator(@Nonnull String bucket, String prefix) {
        if (StringUtils.isBlank(bucket)) {
            log.error("[collectAllS3ObjectIterator] 桶名不能为空 bucket={}", bucket);
            throw new IllegalArgumentException("bucket不能为空");
        }

        return new Iterator<S3ObjectSummary>() {
            private String continuationToken = null;
            private Iterator<S3ObjectSummary> pageIterator = null;
            private boolean isFinished = false;

            /**
             * 加载下一页数据
             */
            private void loadNextPage() {
                if (isFinished) {
                    return;
                }
                ListObjectsV2Request request = new ListObjectsV2Request()
                        .withBucketName(bucket)
                        .withPrefix(prefix)
                        .withContinuationToken(continuationToken);

                ListObjectsV2Result result;
                try {
                    result = s3FileClient.listObjectsV2(request);
                } catch (Exception e) {
                    log.error("[collectAllS3ObjectIterator] listObjectsV2查询异常 bucket={},prefix={}", bucket, prefix, e);
                    isFinished = true;
                    pageIterator = null;
                    throw e;
                }

                // 过滤：排除目录占位对象(key以/结尾，minio/s3虚拟目录标记)
                pageIterator = result.getObjectSummaries().stream()
                        .filter(summary -> !summary.getKey().endsWith("/"))
                        .iterator();

                continuationToken = result.getNextContinuationToken();
                if (StringUtils.isBlank(continuationToken)) {
                    isFinished = true;
                }
                log.debug("[collectAllS3ObjectIterator] 加载一页完成 bucket={},prefix={},本页有效文件数量={},hasNextToken={}",
                        bucket, prefix, result.getObjectSummaries().size(), StringUtils.isNotBlank(continuationToken));
            }

            @Override
            public boolean hasNext() {
                // 当前页还有数据直接返回true
                if (pageIterator != null && pageIterator.hasNext()) {
                    return true;
                }
                // 已经全部拉取完毕
                if (isFinished) {
                    return false;
                }
                // 加载下一页
                loadNextPage();
                // 加载完判断本页是否存在元素
                return pageIterator != null && pageIterator.hasNext();
            }

            @Override
            public S3ObjectSummary next() {
                if (!hasNext()) {
                    throw new NoSuchElementException("没有更多S3对象");
                }
                return pageIterator.next();
            }
        };
    }

    /**
     * 从sourceBucketName目录下移动文件，打包为zip存入目标桶指定路径
     * 逻辑：遍历源目录文件 → 过滤后缀 → 读取文件打包成zip → 上传zip到目标桶 → 删除源目录原始文件
     *
     * @param sourceBucketName 原桶名称
     * @param sourceFolderPath 【文件夹】源路径 案例：/folder1/test1
     * @param targetBucketName 目标桶名称
     * @param targetZipPath    【文件】目标zip路径 案例：/folder2/test3.zip
     * @param skipFileType     需要跳过的文件后缀 如 [".tmp", ".log"]
     */
    @Override
    public void moveDirectoryToZip(String sourceBucketName, String sourceFolderPath,
                                   String targetBucketName, String targetZipPath, List<String> skipFileType) {

    }

    @Override
    public void copyFile(String bucketName, String sourceFilePath, String targetFilePath) {
        this.copyFile(bucketName, sourceFilePath, bucketName, targetFilePath);
    }

    @Override
    public void copyFile(String sourceBucketName, String sourceFilePath, String targetBucketName, String targetFilePath) {
        log.info("[Minio copyFile] sourceBucket={},sourceKey={},targetBucket={},targetKey={}",
                sourceBucketName, sourceFilePath, targetBucketName, targetFilePath);
        // 防护：禁止拷贝到自身
        if (Objects.equals(sourceBucketName, targetBucketName)
                && Objects.equals(sourceFilePath, targetFilePath)) {
            log.warn("[Minio copyFile] source and target is same,skip copy self,bucket={},key={}", sourceBucketName, sourceFilePath);
            return;
        }
        try {
            copySingleObject(sourceBucketName, sourceFilePath, targetBucketName, targetFilePath);
        } catch (AmazonS3Exception e) {
            // MinIO磁盘满507特殊识别
            if ("XMinioStorageFull".equals(e.getErrorCode()) || 507 == e.getStatusCode()) {
                log.error("[Minio copyFile] 存储后端磁盘空间不足 507 XMinioStorageFull", e);
                throw new RuntimeException("存储后端磁盘空间不足，请清理存储后重试", e);
            }
            log.error("[Minio copyFile] s3 copy failed sourceBucket={},sourceKey={},targetBucket={},targetKey={}",
                    sourceBucketName, sourceFilePath, targetBucketName, targetFilePath, e);
            throw e;
        } catch (Exception e) {
            log.error("[Minio copyFile] copy exception", e);
            throw new RuntimeException("[Minio copyFile] copy exception", e);
        }
    }

    /**
     * 分批删除对象，强制每批最大1000，解决MalformedXML
     */
    private void batchDeleteByKey(String bucketName, List<String> keyList) {
        if (CollectionUtils.isEmpty(keyList)) {
            return;
        }
        int total = keyList.size();
        for (int start = 0; start < total; start += S3_DELETE_OBJECTS_MAX_BATCH) {
            int end = Math.min(start + S3_DELETE_OBJECTS_MAX_BATCH, total);
            // subList是视图，toArray传给withKeys，安全
            List<String> batchKeys = keyList.subList(start, end);

            DeleteObjectsRequest deleteRequest = new DeleteObjectsRequest(bucketName)
                    .withKeys(batchKeys.toArray(new String[0]))
                    .withQuiet(true);

            log.debug("[S3批量删除] batchIndex:{},batchSize:{}", start / S3_DELETE_OBJECTS_MAX_BATCH + 1, batchKeys.size());
            try {
                s3FileClient.deleteObjects(deleteRequest);
            } catch (MultiObjectDeleteException e) {
                log.error("[S3批量删除部分失败] bucket={},failCount={}", bucketName, e.getErrors().size());
                for (MultiObjectDeleteException.DeleteError err : e.getErrors()) {
                    log.error("[S3单key删除失败] key={},code={},msg={}", err.getKey(), err.getCode(), err.getMessage());
                }
                // 抛出异常，上层感知目录删除不完全
                throw new RuntimeException("S3批量删除部分对象失败", e);
            }
        }
    }
}
