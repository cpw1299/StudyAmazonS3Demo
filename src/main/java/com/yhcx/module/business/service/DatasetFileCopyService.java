package com.yhcx.module.business.service;

import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.yhcx.module.business.dal.dataobject.DatasetRecordInfoDO;
import com.yhcx.module.business.dal.mysql.DatasetRecordInfoMapper;
import com.yhcx.module.business.framework.AmazonS3Properties;
import com.yhcx.module.business.framework.DatasetCopyProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
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
import java.util.Map;

/**
 * dataset_record_info -> ds_dataset 的文件复制服务。
 *
 * <p>本模块只负责读取 dataset_record_info 和复制对象，不查询或新增 ds_dataset。
 * 调用方创建 ds_dataset 后，将其主键和根路径传入本服务。</p>
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class DatasetFileCopyService {

    private final DatasetRecordInfoMapper datasetRecordInfoMapper;
    private final S3Client sourceS3Client;
    private final AmazonS3Properties sourceProperties;
    private final DatasetCopyProperties copyProperties;
    private final LegacyS3FileStorageTarget targetStorage;

    /**
     * 复制一个 dataset_record_info 对应的全部文件。
     *
     * @param datasetRecordId dataset_record_info.id
     * @param dsDatasetId ds_dataset.id，由调用方创建后传入
     * @param targetRootPath ds_dataset 对应的目标根路径，由调用方传入
     */
    public void copyDatasetFiles(Integer datasetRecordId,
                                 Long dsDatasetId,
                                 String targetRootPath) {
        if (datasetRecordId == null || dsDatasetId == null) {
            throw new IllegalArgumentException("datasetRecordId 和 dsDatasetId 不能为空");
        }
        if (!StringUtils.hasText(targetRootPath)) {
            throw new IllegalArgumentException("targetRootPath 不能为空");
        }
        if (!StringUtils.hasText(copyProperties.getTargetBucket())) {
            throw new IllegalStateException("dataset.copy.target-bucket 未配置");
        }

        DatasetRecordInfoDO record = datasetRecordInfoMapper.selectById(datasetRecordId);
        if (record == null) {
            throw new IllegalArgumentException("dataset_record_info 不存在, id=" + datasetRecordId);
        }
        if (!StringUtils.hasText(record.getDatasetStoragePath())) {
            throw new IllegalArgumentException("dataset_storage_path 不能为空, id=" + datasetRecordId);
        }

        SourcePath sourcePath = resolveSourcePath(record.getDatasetStoragePath());

        log.info("[DatasetCopy] start, datasetRecordId={}, dsDatasetId={}, sourceBucket={}, sourcePrefix={}, targetBucket={}, targetRootPath={}",
                datasetRecordId, dsDatasetId, sourcePath.bucket, sourcePath.prefix,
                copyProperties.getTargetBucket(), targetRootPath);

        targetStorage.createDatasetRoot(copyProperties.getTargetBucket(), targetRootPath, dsDatasetId);

        ListObjectsV2Request request = ListObjectsV2Request.builder()
                .bucket(sourcePath.bucket)
                .prefix(sourcePath.prefix)
                .build();

        long fileCount = 0;
        long totalBytes = 0;

        ListObjectsV2Iterable pages = sourceS3Client.listObjectsV2Paginator(request);
        for (software.amazon.awssdk.services.s3.model.ListObjectsV2Response page : pages) {
            for (S3Object sourceObject : page.contents()) {
                String sourceKey = sourceObject.key();
                if (sourceKey.endsWith("/")) {
                    continue;
                }

                String relativePath = relativePath(sourcePath.prefix, sourceKey);
                String targetKey = joinPath(targetRootPath, relativePath);

                if (targetObjectHasSameSize(targetKey, sourceObject.size())) {
                    log.info("[DatasetCopy] skip existing object, sourceKey={}, targetKey={}, size={}",
                            sourceKey, targetKey, sourceObject.size());
                    fileCount++;
                    totalBytes += sourceObject.size();
                    continue;
                }

                copySingleObject(sourcePath.bucket, sourceKey, sourceObject.size(),
                        targetKey, dsDatasetId);
                fileCount++;
                totalBytes += sourceObject.size();
            }
        }

        log.info("[DatasetCopy] completed, datasetRecordId={}, dsDatasetId={}, fileCount={}, totalBytes={}",
                datasetRecordId, dsDatasetId, fileCount, totalBytes);
    }

    private void copySingleObject(String sourceBucket,
                                  String sourceKey,
                                  long sourceSize,
                                  String targetKey,
                                  Long dsDatasetId) {
        try (ResponseInputStream<GetObjectResponse> sourceStream =
                     sourceS3Client.getObject(GetObjectRequest.builder()
                             .bucket(sourceBucket)
                             .key(sourceKey)
                             .build())) {

            GetObjectResponse response = sourceStream.response();
            long contentLength = response.contentLength() != null
                    ? response.contentLength() : sourceSize;

            targetStorage.upload(
                    copyProperties.getTargetBucket(),
                    targetKey,
                    sourceStream,
                    contentLength,
                    response.contentType(),
                    Map.of("dataset", String.valueOf(dsDatasetId))
            );

            log.info("[DatasetCopy] copied, sourceKey={}, targetKey={}, size={}",
                    sourceKey, targetKey, contentLength);
        } catch (IOException e) {
            throw new IllegalStateException(
                    "复制文件失败, sourceKey=" + sourceKey + ", targetKey=" + targetKey, e);
        }
    }

    private boolean targetObjectHasSameSize(String targetKey, long sourceSize) {
        try {
            ObjectMetadata metadata = targetStorage.getS3Client().getObjectMetadata(
                    copyProperties.getTargetBucket(), targetKey);
            return metadata.getContentLength() == sourceSize;
        } catch (AmazonS3Exception e) {
            if (e.getStatusCode() == 404) {
                return false;
            }
            throw e;
        }
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
        while (normalized.endsWith("/") && !normalized.isEmpty()) {
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
