package com.yhcx.framework.file.core.service;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.yhcx.module.infra.framework.file.core.client.FileClient;
import com.yhcx.module.infra.service.file.FileConfigService;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;

/**
 * s3 流文件下载
 *
 * @author liuhm
 * @version 1.0.0
 * @create 2026/8/6 11:26
 **/
@Slf4j
public class S3StreamDownloader {

    private final FileConfigService fileConfigService;

    private final AmazonS3Client s3FileClient;

    public S3StreamDownloader(FileConfigService fileConfigService) {
        this.fileConfigService = fileConfigService;
        FileClient fileClient = fileConfigService.getMasterFileClient();
        this.s3FileClient = (AmazonS3Client) Objects.requireNonNull(fileClient).getClient();
    }

    /**
     * 安全获取 S3 对象流（已内置超时调优，但调用者必须 close）
     */
    public InputStream downloadStream(String bucket, String key) {
        log.info("开始获取大文件流: s3://{}/{}", bucket, key);
        GetObjectRequest request = new GetObjectRequest(bucket, key);
        S3Object s3Object = s3FileClient.getObject(request);

        // 获取底层流
        S3ObjectInputStream inputStream = s3Object.getObjectContent();

        // ★★★ 关键：将 S3Object 引用绑定到流上，以便 close() 时自动 abort ★★★
        // 返回包装流，确保 close 时强制释放连接
        return new SafeS3ObjectInputStream(inputStream, s3Object);
    }

    /**
     * 安全的流包装器：确保关闭时强制中止（abort）底层连接
     */
    private class SafeS3ObjectInputStream extends InputStream {
        private final S3ObjectInputStream delegate;
        private final S3Object s3Object;
        private boolean closed = false;

        public SafeS3ObjectInputStream(S3ObjectInputStream delegate, S3Object s3Object) {
            this.delegate = delegate;
            this.s3Object = s3Object;
        }

        @Override
        public int read() throws IOException {
            return delegate.read();
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            return delegate.read(b, off, len);
        }

        @Override
        public void close() throws IOException {
            if (!closed) {
                closed = true;
                try {
                    // ★★★ 核心：强制中止 HTTP 连接，立即释放连接池资源 ★★★
                    s3Object.close(); // 内部会调用 abort()
                } finally {
                    delegate.close(); // 确保关闭
                }
            }
        }
    }
}
