package com.yhcx.module.business.service;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.yhcx.framework.file.core.service.S3FileStorageService;
import com.yhcx.module.business.dal.dataobject.DatasetRecordInfoDO;
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

    @Value("${yhcx.minio-bucket-name:datax}")
    private String minioBucketName;

    private AmazonS3Client getS3Client() {
        return s3FileStorageService.getS3Client();
    }

    /**
     * 批量复制 dataset_record_info 对应的全部文件。
     *
     * <p>source 和 target 已由上游同步服务组装到 SourceTargetBO 中，
     * 本服务不再通过 dataset_record_info.id 查询源数据。</p>
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
        /*
         * 文件路径映射：
         *
         * source（旧数据集）：datacentermgr-web/PRIVATE/20/19/
         *   - datacentermgr-web/：旧系统固定前缀
         *   - PRIVATE/20/19/：当前数据集的动态目录
         *
         * target（新数据集）：/dataset/10/2/3/9-18标注
         *   - /dataset/10/：新系统固定前缀
         *   - 2/3/9-18标注：当前数据集的动态目录
         *
         * 复制时不是简单地把 sourceKey 整体拼到 targetRootPath。
         * sourcePrefix 会先被去掉，只保留“数据集目录下面的相对路径”，
         * 再把这个相对路径拼到 targetRootPath。
         *
         * 例如：
         *   source object : datacentermgr-web/PRIVATE/20/19/images/a.jpg
         *   relativePath  : images/a.jpg
         *   target object : dataset/10/2/3/9-18标注/images/a.jpg
         *
         * 核心规则：
         *   source 数据集目录 + 相对文件路径
         *                    ↓
         *   target 数据集目录 + 相对文件路径
         *
         * 因此旧系统的固定前缀 datacentermgr-web/ 不会被复制到新系统。
         */

        Long dsDatasetId = target.getId();
        String targetRootPath = target.getRepositoryPath();

        if (source.getId() == null || dsDatasetId == null) {
            throw new IllegalArgumentException("source.id 和 target.id 不能为空");
        }
        if (!StringUtils.hasText(targetRootPath)) {
            throw new IllegalArgumentException("target.storageDir 不能为空, dsDatasetId=" + dsDatasetId);
        }
        if (!StringUtils.hasText(minioBucketName)) {
            throw new IllegalStateException("dataset.copy.target-bucket 未配置");
        }
        if (!StringUtils.hasText(source.getDatasetStoragePath())) {
            throw new IllegalArgumentException(
                    "dataset_storage_path 不能为空, id=" + source.getId());
        }

        SourcePath sourcePath = resolveSourcePath(source.getDatasetStoragePath());

        log.info("[DatasetCopy] start, datasetRecordId={}, dsDatasetId={}, sourceBucket={}, sourcePrefix={}, targetBucket={}, targetRootPath={}",
                source.getId(), dsDatasetId, sourcePath.bucket, sourcePath.prefix,
                minioBucketName, targetRootPath);

        ListObjectsV2Request request = ListObjectsV2Request.builder()
                .bucket(sourcePath.bucket)
                .prefix(sourcePath.prefix)
                .build();

        long fileCount = 0;
        long totalBytes = 0;
        AmazonS3Client targetS3Client = this.getS3Client();

        // ① 先从 source S3/MinIO 获取对象列表。
        // 这里只拿到 Key、Size 等对象元数据，并没有把文件正文加载进 Java 内存。
        ListObjectsV2Iterable pages = sourceS3Client.listObjectsV2Paginator(request);
        for (software.amazon.awssdk.services.s3.model.ListObjectsV2Response page : pages) {
            for (S3Object sourceObject : page.contents()) {
                String sourceKey = sourceObject.key();
                if (sourceKey.endsWith("/")) {
                    continue;
                }

                String relativePath = relativePath(sourcePath.prefix, sourceKey);
                String targetKey = joinPath(targetRootPath, relativePath);

                // ② 目标端幂等检查：如果 target 已有同路径且同大小的对象，就不再传输文件内容。
                if (targetObjectHasSameSize(targetS3Client, minioBucketName, targetKey, sourceObject.size())) {
                    log.info("[DatasetCopy] skip existing object, sourceKey={}, targetKey={}, size={}",
                            sourceKey, targetKey, sourceObject.size());
                    fileCount++;
                    totalBytes += sourceObject.size();
                    continue;
                }

                // ③ 真正复制文件。这里不是 S3 server-side copy，而是：
                //    source S3/MinIO -> Java InputStream -> target S3/MinIO。
                copySingleObject(sourcePath.bucket, minioBucketName, sourceKey, sourceObject.size(),
                        targetKey, dsDatasetId);
                fileCount++;
                totalBytes += sourceObject.size();
            }
        }

        log.info("[DatasetCopy] completed, datasetRecordId={}, dsDatasetId={}, fileCount={}, totalBytes={}",
                source.getId(), dsDatasetId, fileCount, totalBytes);
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
