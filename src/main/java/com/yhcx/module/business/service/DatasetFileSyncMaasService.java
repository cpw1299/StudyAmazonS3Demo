package com.yhcx.module.business.service;

import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.yhcx.module.business.dal.dataobject.DatasetRecordInfoDO;
import com.yhcx.module.business.framework.AmazonS3Properties;
import com.yhcx.module.business.framework.DatasetCopyProperties;
import com.yhcx.module.business.service.bo.SourceTargetBO;
import com.yhcx.module.business.vo.DatasetMaasSaveReqVO;
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
    private final DatasetCopyProperties copyProperties;
    private final LegacyS3FileStorageTarget targetStorage;

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
        // source 旧数据集的文件路径：datacentermgr-web/PRIVATE/20/19/，这三个层级必然存在 PRIVATE/20/19/，是动态值
        // source 旧数据集的文件路径中 datacentermgr-web/ 是固定值
        // target 新数据集的文件路径：/dataset/10/2/3/9-18标注
        // target 新数据集的文件路径中 /dataset/10/ 是固定值，这三个层级必然存在 2/3/9-18标注，是动态值

        Long dsDatasetId = target.getId();
        String targetRootPath = target.getStorageDir();

        if (source.getId() == null || dsDatasetId == null) {
            throw new IllegalArgumentException("source.id 和 target.id 不能为空");
        }
        if (!StringUtils.hasText(targetRootPath)) {
            throw new IllegalArgumentException("target.storageDir 不能为空, dsDatasetId=" + dsDatasetId);
        }
        if (!StringUtils.hasText(copyProperties.getTargetBucket())) {
            throw new IllegalStateException("dataset.copy.target-bucket 未配置");
        }
        if (!StringUtils.hasText(source.getDatasetStoragePath())) {
            throw new IllegalArgumentException(
                    "dataset_storage_path 不能为空, id=" + source.getId());
        }

        SourcePath sourcePath = resolveSourcePath(source.getDatasetStoragePath());

        log.info("[DatasetCopy] start, datasetRecordId={}, dsDatasetId={}, sourceBucket={}, sourcePrefix={}, targetBucket={}, targetRootPath={}",
                source.getId(), dsDatasetId, sourcePath.bucket, sourcePath.prefix,
                copyProperties.getTargetBucket(), targetRootPath);

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
                source.getId(), dsDatasetId, fileCount, totalBytes);
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
