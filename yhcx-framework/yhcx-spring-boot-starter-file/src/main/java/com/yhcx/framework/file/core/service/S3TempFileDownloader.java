package com.yhcx.framework.file.core.service;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.transfer.Download;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.yhcx.module.infra.framework.file.core.client.FileClient;
import com.yhcx.module.infra.service.file.FileConfigService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.Objects;
import java.util.concurrent.Executors;

/**
 * s3 流文件下载
 *
 * @author liuhm
 * @version 1.0.0
 * @create 2026/8/6 11:26
 **/
@Slf4j
@RequiredArgsConstructor
public class S3TempFileDownloader {

    private final FileConfigService fileConfigService;

    private final AmazonS3Client s3FileClient;

    public S3TempFileDownloader(FileConfigService fileConfigService) {
        this.fileConfigService = fileConfigService;
        FileClient fileClient = fileConfigService.getMasterFileClient();
        this.s3FileClient = (AmazonS3Client) Objects.requireNonNull(fileClient).getClient();
    }

    /**
     * 下载 S3 对象到临时文件，返回一个在 close() 时自动删除文件的 InputStream
     *
     * @param bucket 存储桶
     * @param key    对象键
     * @return AutoDeleteFileInputStream（必须用 try-with-resources 关闭）
     * @throws IOException 下载或创建临时文件失败
     */
    public InputStream downloadToTempStream(String bucket, String key) throws IOException {
        log.info("开始下载 s3://{}/{} 到临时文件", bucket, key);

        // 1. 创建临时文件（前缀 "s3tmp_", 后缀 ".tmp"）
        File tempFile = Files.createTempFile("s3tmp_", ".tmp").toFile();
        // 确保 JVM 退出时删除（作为最后保障，但主要靠流的 close）
        tempFile.deleteOnExit();

        TransferManager tm = null;
        try {
            // 2. 创建 TransferManager
            tm = TransferManagerBuilder.standard()
                .withS3Client(s3FileClient)
                .withExecutorFactory(() -> Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors()))
                .build();

            // 3. 执行下载（阻塞等待完成）
            Download download = tm.download(bucket, key, tempFile);
            download.waitForCompletion();

            log.info("下载完成，临时文件：{}，大小：{} 字节", tempFile.getAbsolutePath(), tempFile.length());

            // 4. 返回包装流（关闭时自动删除文件）
            return new AutoDeleteFileInputStream(tempFile);

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            // 如果被中断，删除临时文件并抛出异常
            deleteFileSilently(tempFile);
            throw new IOException("下载被中断", e);
        } catch (Exception e) {
            // 下载失败，清理临时文件
            deleteFileSilently(tempFile);
            throw new IOException("下载失败：" + e.getMessage(), e);
        } finally {
            // 5. 关闭 TransferManager 释放资源（但已下载完成的文件不受影响）
            if (tm != null) {
                tm.shutdownNow(false);
            }
        }
    }

    /**
     * 内部类：FileInputStream 的包装，close() 时删除文件
     */
    private static class AutoDeleteFileInputStream extends FileInputStream {
        private final File file;

        public AutoDeleteFileInputStream(File file) throws IOException {
            super(file);
            this.file = file;
        }

        @Override
        public void close() throws IOException {
            try {
                super.close();  // 先关闭流
            } finally {
                // 无论是否有异常，都删除文件
                deleteFileSilently(file);
                log.debug("临时文件已删除：{}", file.getAbsolutePath());
            }
        }
    }

    /**
     * 静默删除文件（忽略异常）
     */
    private static void deleteFileSilently(File file) {
        if (file != null && file.exists()) {
            if (!file.delete()) {
                log.warn("无法删除临时文件：{}", file.getAbsolutePath());
                // 可以尝试在 JVM 退出时删除
                file.deleteOnExit();
            }
        }
    }
}
