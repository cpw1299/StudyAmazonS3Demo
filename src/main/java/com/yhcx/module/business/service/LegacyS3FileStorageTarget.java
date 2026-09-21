package com.yhcx.module.business.service;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.yhcx.framework.file.core.service.S3FileStorageService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.StringUtils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 旧版 S3FileStorageService 的目标端适配器。
 *
 * <p>当前需求只负责 MinIO A -> MinIO B 的复制，因此暂时复用旧模块的目标 S3Client。
 * 后续替换大文件上传能力时，只需要替换本适配器，不影响 DatasetFileCopyService。</p>
 */
@Slf4j
@RequiredArgsConstructor
public class LegacyS3FileStorageTarget {

    private static final long MAX_PUT_OBJECT_SIZE = 5L * 1024 * 1024 * 1024;
    private static final int MULTIPART_PART_SIZE = 128 * 1024 * 1024;

    private final S3FileStorageService s3FileStorageService;

    /**
     * 旧版 putObject(String, String, String, Map) 的实际语义是创建 0 字节目录对象，
     * 因此这里用于创建 dataset 根目录并写入 metadata。
     */
    public void createDatasetRoot(String bucketName, String rootPath, Long datasetId) {
        if (!StringUtils.hasText(bucketName) || !StringUtils.hasText(rootPath)) {
            throw new IllegalArgumentException("目标 bucket 和 rootPath 不能为空");
        }

        s3FileStorageService.putObject(
                bucketName,
                normalize(rootPath),
                "/",
                Map.of("dataset", String.valueOf(datasetId))
        );
    }

    /**
     * 将源 S3 输入流上传到目标 MinIO。
     * 5GB 以内使用 PutObject，超过 5GB 使用 Multipart Upload。
     */
    public void upload(String bucketName,
                       String targetKey,
                       InputStream inputStream,
                       long contentLength,
                       String contentType,
                       Map<String, String> metadata) throws IOException {
        if (!StringUtils.hasText(bucketName) || !StringUtils.hasText(targetKey)) {
            throw new IllegalArgumentException("目标 bucket 和 key 不能为空");
        }
        if (contentLength < 0) {
            throw new IllegalArgumentException("文件大小不能小于 0");
        }

        AmazonS3Client targetClient = s3FileStorageService.getS3Client();
        if (contentLength <= MAX_PUT_OBJECT_SIZE) {
            uploadByPutObject(targetClient, bucketName, targetKey, inputStream,
                    contentLength, contentType, metadata);
        } else {
            uploadByMultipart(targetClient, bucketName, targetKey, inputStream,
                    contentLength, contentType, metadata);
        }
    }

    private void uploadByPutObject(AmazonS3Client client,
                                   String bucketName,
                                   String targetKey,
                                   InputStream inputStream,
                                   long contentLength,
                                   String contentType,
                                   Map<String, String> metadata) {
        ObjectMetadata objectMetadata = buildMetadata(contentLength, contentType, metadata);
        client.putObject(new PutObjectRequest(
                bucketName, targetKey, inputStream, objectMetadata));
    }

    private void uploadByMultipart(AmazonS3Client client,
                                   String bucketName,
                                   String targetKey,
                                   InputStream inputStream,
                                   long contentLength,
                                   String contentType,
                                   Map<String, String> metadata) throws IOException {
        ObjectMetadata objectMetadata = buildMetadata(null, contentType, metadata);
        InitiateMultipartUploadResult initResult = client.initiateMultipartUpload(
                new InitiateMultipartUploadRequest(bucketName, targetKey)
                        .withObjectMetadata(objectMetadata));

        String uploadId = initResult.getUploadId();
        List<PartETag> partETags = new ArrayList<>();
        byte[] buffer = new byte[MULTIPART_PART_SIZE];
        long remaining = contentLength;
        int partNumber = 1;

        try {
            while (remaining > 0) {
                int expected = (int) Math.min(MULTIPART_PART_SIZE, remaining);
                int offset = 0;
                while (offset < expected) {
                    int read = inputStream.read(buffer, offset, expected - offset);
                    if (read < 0) {
                        throw new IOException("源文件流提前结束，expected=" + contentLength
                                + ", uploaded=" + (contentLength - remaining + offset));
                    }
                    if (read > 0) {
                        offset += read;
                    }
                }

                PartETag partETag = client.uploadPart(new UploadPartRequest()
                        .withBucketName(bucketName)
                        .withKey(targetKey)
                        .withUploadId(uploadId)
                        .withPartNumber(partNumber)
                        .withInputStream(new ByteArrayInputStream(buffer, 0, expected))
                        .withPartSize(expected))
                        .getPartETag();

                partETags.add(partETag);
                remaining -= expected;
                partNumber++;
            }

            client.completeMultipartUpload(new CompleteMultipartUploadRequest(
                    bucketName, targetKey, uploadId, partETags));
        } catch (Exception e) {
            try {
                client.abortMultipartUpload(bucketName, targetKey, uploadId);
            } catch (Exception abortException) {
                log.error("[DatasetCopy] abort target multipart upload failed, bucket={}, key={}, uploadId={}",
                        bucketName, targetKey, uploadId, abortException);
            }
            if (e instanceof IOException) {
                throw (IOException) e;
            }
            throw new IOException("目标 MinIO Multipart Upload 失败: " + targetKey, e);
        }
    }

    private ObjectMetadata buildMetadata(Long contentLength,
                                         String contentType,
                                         Map<String, String> metadata) {
        ObjectMetadata objectMetadata = new ObjectMetadata();
        if (contentLength != null) {
            objectMetadata.setContentLength(contentLength);
        }
        if (StringUtils.hasText(contentType)) {
            objectMetadata.setContentType(contentType);
        }
        if (metadata != null && !metadata.isEmpty()) {
            objectMetadata.setUserMetadata(metadata);
        }
        return objectMetadata;
    }

    private String normalize(String path) {
        String normalized = path.trim().replace('\\', '/');
        while (normalized.startsWith("/")) {
            normalized = normalized.substring(1);
        }
        while (normalized.endsWith("/") && normalized.length() > 1) {
            normalized = normalized.substring(0, normalized.length() - 1);
        }
        return normalized;
    }
}
