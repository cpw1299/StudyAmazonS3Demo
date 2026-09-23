package com.yhcx.module.business.service;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.yhcx.framework.file.core.service.S3FileStorageService;
import com.yhcx.module.business.dal.dataobject.DatasetRecordInfoDO;
import com.yhcx.module.business.dal.dataobject.DatasetFileSyncDetailDO;
import com.yhcx.module.business.dal.dataobject.DatasetFileSyncRecordDO;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import java.util.concurrent.TimeUnit;

import com.yhcx.module.business.framework.AmazonS3Properties;
import com.yhcx.module.business.service.bo.SourceTargetBO;
import com.yhcx.module.business.vo.DatasetMaasSaveReqVO;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

/**
 * dataset_record_info -> ds_dataset 的文件复制服务。
 *
 * <p>本服务只负责根据调用方传入的 source/target 信息复制对象，
 * 不再查询 dataset_record_info，也不查询或新增 ds_dataset。</p>
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class DatasetFileSyncMaasService {

    private final S3Client sourceS3Client;
    private final AmazonS3Properties sourceProperties;
    private final S3FileStorageService s3FileStorageService;
    private final DatasetFileSyncRecordService recordService;
    private final RedissonClient redissonClient;

    @Value("${yhcx.minio-bucket-name:datax}")
    private String minioBucketName;

    private AmazonS3Client getS3Client() {
        return s3FileStorageService.getS3Client();
    }

    /**
     * 批量复制 dataset_record_info 对应的全部文件。
     *
     * <p>每个数据集独立记录同步任务和文件明细。单个文件失败不会中断其它文件，
     * 再次触发时只处理尚未成功的文件。</p>
     */
    public void copyDatasetFiles(List<SourceTargetBO> boList) {
        if (boList == null || boList.isEmpty()) {
            return;
        }
        for (SourceTargetBO bo : boList) {
            copyDatasetFiles(bo);
        }
    }

    private void copyDatasetFiles(SourceTargetBO bo) {
        if (bo == null || bo.getSource() == null || bo.getTarget() == null) {
            throw new IllegalArgumentException("sourceTargetBO、source、target 不能为空");
        }

        DatasetRecordInfoDO source = bo.getSource();
        DatasetMaasSaveReqVO target = bo.getTarget();
        Long sourceDatasetId = source.getId();
        Long targetDatasetId = target.getId();
        if (sourceDatasetId == null || targetDatasetId == null) {
            throw new IllegalArgumentException("source.id 和 target.id 不能为空");
        }

        String lockKey = "dataset:file:sync:lock:" + sourceDatasetId + ":" + targetDatasetId;
        RLock lock = redissonClient.getLock(lockKey);
        boolean locked = false;
        try {
            locked = lock.tryLock(0, TimeUnit.SECONDS);
            if (!locked) {
                log.warn("[DatasetCopy] another sync task is running, sourceDatasetId={}, targetDatasetId={}",
                        sourceDatasetId, targetDatasetId);
                return;
            }
            doCopyDatasetFiles(source, target);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("获取数据集文件同步锁被中断", e);
        } finally {
            if (locked && lock.isHeldByCurrentThread()) {
                lock.unlock();
            }
        }
    }

    private void doCopyDatasetFiles(DatasetRecordInfoDO source, DatasetMaasSaveReqVO target) {
        Long dsDatasetId = target.getId();
        String targetRootPath = target.getRepositoryPath();
        if (!StringUtils.hasText(targetRootPath)) {
            throw new IllegalArgumentException("target.storageDir 不能为空, dsDatasetId=" + dsDatasetId);
        }
        if (!StringUtils.hasText(minioBucketName)) {
            throw new IllegalStateException("dataset.copy.target-bucket 未配置");
        }
        if (!StringUtils.hasText(source.getDatasetStoragePath())) {
            throw new IllegalArgumentException("dataset_storage_path 不能为空, id=" + source.getId());
        }

        SourcePath sourcePath = resolveSourcePath(source.getDatasetStoragePath());
        DatasetFileSyncRecordDO record = recordService.getOrCreate(source.getId(), dsDatasetId);
        if (DatasetFileSyncRecordService.STATUS_SUCCESS.equals(record.getStatus())) {
            log.info("[DatasetCopy] already completed, datasetRecordId={}, dsDatasetId={}", source.getId(), dsDatasetId);
            return;
        }
        recordService.markProcessing(record);

        try {
            log.info("[DatasetCopy] start, datasetRecordId={}, dsDatasetId={}, sourceBucket={}, sourcePrefix={}, targetBucket={}, targetRootPath={}",
                    source.getId(), dsDatasetId, sourcePath.bucket, sourcePath.prefix, minioBucketName, targetRootPath);

            AmazonS3Client targetS3Client = this.getS3Client();
            ListObjectsV2Request request = ListObjectsV2Request.builder()
                    .bucket(sourcePath.bucket)
                    .prefix(sourcePath.prefix)
                    .build();

            // 第一遍只统计文件总数，避免把大量 S3Object 元数据全部放进 JVM 内存。
            long totalFileCount = countSourceFiles(request, sourcePath);
            recordService.rebuildProgress(record, totalFileCount);
            if (DatasetFileSyncRecordService.STATUS_SUCCESS.equals(record.getStatus())) {
                log.info("[DatasetCopy] all files already completed, datasetRecordId={}, dsDatasetId={}, fileCount={}",
                        source.getId(), dsDatasetId, totalFileCount);
                return;
            }
            if (totalFileCount == 0) {
                recordService.markSuccess(record);
                log.info("[DatasetCopy] completed, datasetRecordId={}, dsDatasetId={}, fileCount=0", source.getId(), dsDatasetId);
                return;
            }

            // 第二遍真正处理文件。单个文件异常只记录 FAILED，不影响后续文件。
            ListObjectsV2Iterable pages = sourceS3Client.listObjectsV2Paginator(request);
            for (software.amazon.awssdk.services.s3.model.ListObjectsV2Response page : pages) {
                for (S3Object sourceObject : page.contents()) {
                    String sourceKey = sourceObject.key();
                    if (sourceKey.endsWith("/")) {
                        continue;
                    }

                    String relativePath = relativePath(sourcePath.prefix, sourceKey);
                    String targetKey = joinPath(targetRootPath, relativePath);
                    DatasetFileSyncDetailDO detail = recordService.getOrCreateDetail(
                            record, sourceKey, targetKey, sourceObject.size());

                    if (DatasetFileSyncRecordService.FILE_STATUS_SUCCESS.equals(detail.getStatus())) {
                        continue;
                    }

                    String previousStatus = detail.getStatus();
                    try {
                        recordService.markFilePending(detail);
                        if (targetObjectHasSameSize(targetS3Client, minioBucketName, targetKey, sourceObject.size())) {
                            log.info("[DatasetCopy] skip existing object, sourceKey={}, targetKey={}, size={}",
                                    sourceKey, targetKey, sourceObject.size());
                        } else {
                            copySingleObject(sourcePath.bucket, minioBucketName, sourceKey, sourceObject.size(),
                                    targetKey, dsDatasetId);
                        }
                        recordService.recordFileSuccess(record, detail, previousStatus);
                    } catch (Exception e) {
                        recordService.recordFileFailed(record, detail, previousStatus, e);
                        log.error("[DatasetCopy] file failed, datasetRecordId={}, dsDatasetId={}, sourceKey={}, targetKey={}",
                                source.getId(), dsDatasetId, sourceKey, targetKey, e);
                    }
                }
            }

            recordService.finish(record);
            log.info("[DatasetCopy] completed, datasetRecordId={}, dsDatasetId={}, totalFileCount={}, successFileCount={}, failedFileCount={}",
                    source.getId(), dsDatasetId, record.getTotalFileCount(), record.getSuccessFileCount(), record.getFailedFileCount());
        } catch (Exception e) {
            recordService.markTaskFailed(record, e);
            throw e;
        }
    }

    private long countSourceFiles(ListObjectsV2Request request, SourcePath sourcePath) {
        long totalFileCount = 0;
        ListObjectsV2Iterable pages = sourceS3Client.listObjectsV2Paginator(request);
        for (software.amazon.awssdk.services.s3.model.ListObjectsV2Response page : pages) {
            for (S3Object sourceObject : page.contents()) {
                if (!sourceObject.key().endsWith("/")) {
                    totalFileCount++;
                }
            }
        }
        return totalFileCount;
    }

    private void copySingleObject(String sourceBucket,
                                  String targetBucket,
                                  String sourceKey,
                                  long sourceSize,
                                  String targetKey,
                                  Long dsDatasetId) {
        /*
         * ④ 从 source 读取文件正文：
         *
         *    source S3/MinIO
         *          │ GetObject
         *          ▼
         *    ResponseInputStream<GetObjectResponse>
         *          │
         *          │ read()
         *          ▼
         *    Java 应用
         *
         * getObject() 返回的是流，不是整个文件的 byte[]。
         * 因此文件正文不会先完整落到 Java 堆内存，也不会先写成本地临时文件。
         * sourceStream 会在后续 upload() 读取过程中持续从 source 获取数据。
         */
        try (ResponseInputStream<GetObjectResponse> sourceStream =
                     sourceS3Client.getObject(GetObjectRequest.builder()
                             .bucket(sourceBucket)
                             .key(sourceKey)
                             .build())) {

            // response() 读取的是本次 GetObject 响应的元数据；
            // 文件正文仍然由 sourceStream 按需读取。
            GetObjectResponse response = sourceStream.response();
            long contentLength = response.contentLength() != null
                    ? response.contentLength() : sourceSize;

            /*
             * ⑤ 把 sourceStream 直接交给 targetStorage.upload()。
             *
             *    sourceStream.read()
             *          ↓
             *    targetStorage.upload(InputStream)
             *          ↓
             *    target S3/MinIO
             *
             * upload() 从输入流读取 source 文件内容并写入 targetKey。
             * 整个过程没有“source -> 本地文件 -> target”的中间文件。
             */
            this.upload(
                    targetBucket,
                    targetKey,
                    sourceStream,
                    contentLength,
                    response.contentType(),
                    Map.of("dataset", String.valueOf(dsDatasetId))
            );

            // ⑥ upload() 返回后，目标端上传调用已完成；try-with-resources 随后关闭 sourceStream，
            // 释放 source 端 HTTP 连接及相关资源。
            log.info("[DatasetCopy] copied, sourceKey={}, targetKey={}, size={}",
                    sourceKey, targetKey, contentLength);
        } catch (IOException e) {
            throw new IllegalStateException(
                    "复制文件失败, sourceKey=" + sourceKey + ", targetKey=" + targetKey, e);
        }
    }

    private boolean targetObjectHasSameSize(AmazonS3Client targetS3Client,
                                            String targetBucket,
                                            String targetKey,
                                            long sourceSize) {
        try {
            ObjectMetadata metadata = targetS3Client.getObjectMetadata(targetBucket, targetKey);
            return metadata.getContentLength() == sourceSize;
        } catch (AmazonS3Exception e) {
            if (e.getStatusCode() == 404) {
                return false;
            }
            throw e;
        }
    }

    /**
     * 将源 S3 输入流上传到目标 MinIO。
     *
     * <p>实际上传策略由 S3FileStorageService 决定：5GB 以内普通 PutObject，
     * 超过 5GB 自动 Multipart Upload。整个过程不需要本地临时文件。</p>
     */
    private void upload(String bucketName,
                       String targetKey,
                       InputStream inputStream,
                       long contentLength,
                       String contentType,
                       Map<String, String> metadata) throws IOException {
        if (!StringUtils.hasText(bucketName) || !StringUtils.hasText(targetKey)) {
            throw new IllegalArgumentException("目标 bucket 和 key 不能为空");
        }
        if (inputStream == null) {
            throw new IllegalArgumentException("inputStream 不能为空");
        }
        if (contentLength < 0) {
            throw new IllegalArgumentException("文件大小不能小于 0");
        }

        s3FileStorageService.upload(
                bucketName,
                "",
                targetKey,
                inputStream,
                contentLength,
                contentType,
                metadata
        );
    }

    private SourcePath resolveSourcePath(String storagePath) {
        String path = storagePath.trim().replace('\\', '/');
        if (path.startsWith("s3://")) {
            String withoutScheme = path.substring("s3://".length());
            int slash = withoutScheme.indexOf('/');
            if (slash < 0) {
                return new SourcePath(withoutScheme, "");
            }
            return new SourcePath(withoutScheme.substring(0, slash),
                    normalizePrefix(withoutScheme.substring(slash + 1)));
        }

        if (!StringUtils.hasText(sourceProperties.getDefaultBucket())) {
            throw new IllegalStateException(
                    "aws.s3.default-bucket 未配置，且 dataset_storage_path 不是 s3://bucket/prefix 格式");
        }
        return new SourcePath(sourceProperties.getDefaultBucket(), normalizePrefix(path));
    }

    private String relativePath(String prefix, String objectKey) {
        String normalizedPrefix = normalizePrefix(prefix);
        if (!StringUtils.hasText(normalizedPrefix)) {
            return objectKey;
        }

        String expectedPrefix = normalizedPrefix + "/";
        if (!objectKey.startsWith(expectedPrefix)) {
            throw new IllegalStateException(
                    "对象不在 dataset_storage_path 前缀下: prefix=" + normalizedPrefix + ", key=" + objectKey);
        }
        return objectKey.substring(expectedPrefix.length());
    }

    private String joinPath(String root, String relative) {
        String normalizedRoot = normalizePrefix(root);
        String normalizedRelative = normalizePrefix(relative);
        if (!StringUtils.hasText(normalizedRelative)) {
            return normalizedRoot;
        }
        if (!StringUtils.hasText(normalizedRoot)) {
            return normalizedRelative;
        }
        return normalizedRoot + "/" + normalizedRelative;
    }

    private String normalizePrefix(String value) {
        if (value == null) {
            return "";
        }
        String normalized = value.trim().replace('\\', '/');
        while (normalized.startsWith("/")) {
            normalized = normalized.substring(1);
        }
        while (normalized.endsWith("/")) {
            normalized = normalized.substring(0, normalized.length() - 1);
        }
        return normalized;
    }

    private static final class SourcePath {
        private final String bucket;
        private final String prefix;

        private SourcePath(String bucket, String prefix) {
            this.bucket = bucket;
            this.prefix = prefix;
        }
    }
}
