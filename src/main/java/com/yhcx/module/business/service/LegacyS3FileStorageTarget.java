package com.yhcx.module.business.service;

import com.amazonaws.services.s3.AmazonS3Client;
import com.yhcx.framework.file.core.service.S3FileStorageService;
import lombok.RequiredArgsConstructor;
import org.springframework.util.StringUtils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

/**
 * 新数据集模块文件服务的目标端适配器。
 *
 * <p>复制服务只负责提供源端 InputStream；普通上传与大文件 Multipart Upload
 * 由新数据集模块的 S3FileStorageService 统一处理。</p>
 */
@RequiredArgsConstructor
public class LegacyS3FileStorageTarget {

    private final S3FileStorageService s3FileStorageService;

    public AmazonS3Client getS3Client() {
        return s3FileStorageService.getS3Client();
    }

    /**
     * 将源 S3 输入流上传到目标 MinIO。
     *
     * <p>实际上传策略由 S3FileStorageService 决定：5GB 以内普通 PutObject，
     * 超过 5GB 自动 Multipart Upload。整个过程不需要本地临时文件。</p>
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
}
