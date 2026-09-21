package com.yhcx.module.business.serivce;

import com.amazonaws.HttpMethod;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GeneratePresignedUrlRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;
import com.yhcx.module.business.framework.AmazonS3Properties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.io.File;
import java.io.InputStream;
import java.time.Duration;
import java.util.Date;

@Slf4j
@Service
@RequiredArgsConstructor
public class AmazonS3StorageService {

    private final AmazonS3Client amazonS3Client;
    private final TransferManager transferManager;
    private final AmazonS3Properties props;

    /**
     * 上传本地文件（超过阈值自动分片并发上传，生产推荐）
     */
    public String uploadFile(String key, File file, String contentType) {
        String bucketName = props.getDefaultBucket();
        try {
            ObjectMetadata metadata = new ObjectMetadata();
            if (StringUtils.hasText(contentType)) {
                metadata.setContentType(contentType);
            }
            PutObjectRequest objectRequest = new PutObjectRequest(bucketName, key, file);
            objectRequest.setMetadata(metadata);
            Upload upload = transferManager.upload(objectRequest);
            upload.waitForCompletion(); // 也可不等待，返回 Upload 对象做异步回调/进度监听
            log.info("[S3] 上传完成 bucketName={}, key={}, size={}", bucketName, key, file.length());
            return key;
        } catch (Exception e) {
            throw new RuntimeException("文件上传失败: " + key, e);
        }
    }

    /**
     * 流式上传（必须知道 contentLength）
     */
    public String uploadStream(String key, InputStream in, long contentLength, String contentType) {
        String bucketName = props.getDefaultBucket();
        try {
            ObjectMetadata metadata = new ObjectMetadata();
            metadata.setContentLength(contentLength);
            if (StringUtils.hasText(contentType)) {
                metadata.setContentType(contentType);
            }
            amazonS3Client.putObject(bucketName, key, in, metadata);
            return key;
        } catch (Exception e) {
            throw new RuntimeException("流式上传失败: " + key, e);
        }
    }

    /**
     * 获取对象输入流（调用方负责关闭；未读完时建议先 abort() 释放连接再 close）
     */
    public InputStream getObjectStream(String key) {
        try {
            S3Object object = amazonS3Client.getObject(props.getDefaultBucket(), key);
            return object.getObjectContent();
        } catch (Exception e) {
            throw new RuntimeException("获取对象失败: " + key, e);
        }
    }

    /**
     * 生成预签名下载 URL（前端直连 S3 下载，不经过应用服务器）
     */
    public String generatePresignedUrl(String key, Duration ttl) {
        GeneratePresignedUrlRequest request = new GeneratePresignedUrlRequest(
                props.getDefaultBucket(), key)
                .withMethod(HttpMethod.GET)
                .withExpiration(new Date(System.currentTimeMillis() + ttl.toMillis()));
        return amazonS3Client.generatePresignedUrl(request).toString();
    }

    /**
     * 判断对象是否存在（HEAD 请求）
     */
    public boolean exists(String key) {
        return amazonS3Client.doesObjectExist(props.getDefaultBucket(), key);
    }

    /**
     * 删除对象
     */
    public void delete(String key) {
        amazonS3Client.deleteObject(props.getDefaultBucket(), key);
    }
}
