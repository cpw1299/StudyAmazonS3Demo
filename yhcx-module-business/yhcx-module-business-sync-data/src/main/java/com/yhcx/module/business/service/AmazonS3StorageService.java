package com.yhcx.module.business.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.s3.presigner.model.GetObjectPresignRequest;
import software.amazon.awssdk.services.s3.presigner.model.PresignedGetObjectRequest;
import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.transfer.s3.model.FileUpload;
import software.amazon.awssdk.transfer.s3.model.UploadFileRequest;
import com.yhcx.module.business.framework.AmazonS3Properties;

import java.io.File;
import java.io.InputStream;
import java.time.Duration;

/**
 * AWS SDK for Java 2.x S3 存储服务。
 * <p>
 * 当前阶段：
 * - 普通文件上传已经切到 S3TransferManager v2；
 * - 普通流上传使用 S3Client；
 * - 后续 800GB 大文件会在此基础上增加可暂停/恢复、进度、校验和、失败清理等能力；
 * - ZIP 解压暂不在本次迁移中处理。
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class AmazonS3StorageService {

    private final S3Client s3Client;
    private final S3TransferManager s3TransferManager;
    private final S3Presigner s3Presigner;
    private final AmazonS3Properties props;

    /**
     * 上传本地文件。
     * <p>
     * v2 使用 S3TransferManager + CRT S3AsyncClient：
     * - 大于 multipart threshold 自动 multipart；
     * - multipart part 并行上传；
     * - 支持后续 pause/resume 扩展。
     */
    public String uploadFile(String key, File file, String contentType) {
        String bucketName = props.getDefaultBucket();

        if (!file.isFile()) {
            throw new IllegalArgumentException("文件不存在或不是普通文件: " + file);
        }

        try {
            UploadFileRequest.Builder builder = UploadFileRequest.builder()
                    .putObjectRequest(request -> {
                        request.bucket(bucketName)
                                .key(key);

                        if (StringUtils.hasText(contentType)) {
                            request.contentType(contentType);
                        }
                    })
                    .source(file.toPath());

            FileUpload upload = s3TransferManager.uploadFile(builder.build());

            // 当前 API 保持原有同步语义；后续大文件版本可以直接返回 transferId。
            upload.completionFuture().join();

            log.info(
                    "[S3] 上传完成 bucket={}, key={}, sizeBytes={}",
                    bucketName,
                    key,
                    file.length());

            return key;
        } catch (Exception e) {
            throw new RuntimeException("文件上传失败: " + key, e);
        }
    }

    /**
     * 流式上传。
     * <p>
     * 适用于普通流场景；800GB 文件不建议把整个文件通过应用层同步流式转发，
     * 后续大文件方案会使用 Transfer Manager 的 multipart/resumable 能力。
     */
    public String uploadStream(String key, InputStream in, long contentLength, String contentType) {
        String bucketName = props.getDefaultBucket();

        try {
            PutObjectRequest.Builder requestBuilder = PutObjectRequest.builder()
                    .bucket(bucketName)
                    .key(key)
                    .contentLength(contentLength);

            if (StringUtils.hasText(contentType)) {
                requestBuilder.contentType(contentType);
            }

            s3Client.putObject(
                    requestBuilder.build(),
                    RequestBody.fromInputStream(in, contentLength));

            return key;
        } catch (Exception e) {
            throw new RuntimeException("流式上传失败: " + key, e);
        }
    }

    /**
     * 获取对象输入流。
     * 调用方必须负责关闭返回的 ResponseInputStream。
     */
    public InputStream getObjectStream(String key) {
        try {
            ResponseInputStream<GetObjectResponse> stream = s3Client.getObject(
                    GetObjectRequest.builder()
                            .bucket(props.getDefaultBucket())
                            .key(key)
                            .build());

            return stream;
        } catch (Exception e) {
            throw new RuntimeException("获取对象失败: " + key, e);
        }
    }

    /**
     * 生成预签名下载 URL。
     * <p>
     * S3Presigner 官方限制单个预签名请求最长 7 天。
     */
    public String generatePresignedUrl(String key, Duration ttl) {
        if (ttl.isNegative() || ttl.isZero()) {
            throw new IllegalArgumentException("预签名 URL TTL 必须大于 0");
        }

        try {
            GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                    .bucket(props.getDefaultBucket())
                    .key(key)
                    .build();

            GetObjectPresignRequest presignRequest = GetObjectPresignRequest.builder()
                    .signatureDuration(ttl)
                    .getObjectRequest(getObjectRequest)
                    .build();

            PresignedGetObjectRequest presigned = s3Presigner.presignGetObject(presignRequest);
            return presigned.url().toString();
        } catch (Exception e) {
            throw new RuntimeException("生成预签名 URL 失败: " + key, e);
        }
    }

    /**
     * 判断对象是否存在。
     */
    public boolean exists(String key) {
        try {
            s3Client.headObject(HeadObjectRequest.builder()
                    .bucket(props.getDefaultBucket())
                    .key(key)
                    .build());
            return true;
        } catch (NoSuchKeyException e) {
            return false;
        } catch (S3Exception e) {
            if (e.statusCode() == 404) {
                return false;
            }
            throw e;
        }
    }

    /**
     * 删除对象。
     */
    public void delete(String key) {
        s3Client.deleteObject(DeleteObjectRequest.builder()
                .bucket(props.getDefaultBucket())
                .key(key)
                .build());
    }
}
