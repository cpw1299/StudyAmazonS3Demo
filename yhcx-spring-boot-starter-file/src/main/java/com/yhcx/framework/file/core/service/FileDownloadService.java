package com.yhcx.framework.file.core.service;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.yhcx.framework.common.util.file.Tools;
import com.yhcx.module.infra.framework.file.core.client.FileClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.*;
import java.util.Objects;

public class FileDownloadService {

    private final Logger log = LoggerFactory.getLogger(FileDownloadService.class);

    /**
     * 1MB Buffer。
     * <p>
     * 大文件下载不需要随着文件大小增加 Buffer。
     */
    private static final int BUFFER_SIZE = 1024 * 1024;

    private final AmazonS3Client amazonS3Client;

    public FileDownloadService(FileClient fileClient) {
        this.amazonS3Client = (AmazonS3Client) Objects.requireNonNull(fileClient).getClient();
    }


    /**
     * 从 MinIO 下载文件到本地磁盘。
     *
     * @param bucketName MinIO Bucket
     * @param pathKey MinIO Object Key
     * @param targetPath 本地目标文件
     */
    public Path download(String bucketName, String pathKey, Path targetPath) throws IOException {

        if (bucketName == null || bucketName.isBlank()) {
            throw new IllegalArgumentException("bucketName 不能为空");
        }

        if (pathKey == null || pathKey.isBlank()) {
            throw new IllegalArgumentException("pathKey 不能为空");
        }

        if (targetPath == null) {
            throw new IllegalArgumentException("targetPath 不能为空");
        }

        Path absoluteTarget = targetPath.toAbsolutePath().normalize();

        Path parent = absoluteTarget.getParent();

        if (parent != null) {
            Files.createDirectories(parent);
        }

        /*
         * 不直接写目标文件。
         *
         * 例如目标：
         *
         * /data/zip/test.zip
         *
         * 实际先写：
         *
         * /data/zip/test.zip.download
         *
         * 下载成功后再移动成：
         *
         * /data/zip/test.zip
         */
        Path tempPath = absoluteTarget.resolveSibling(absoluteTarget.getFileName() + ".download");

        /*
         * 如果之前存在失败残留，删除。
         */
        Files.deleteIfExists(tempPath);

        long startTime = System.currentTimeMillis();

        long expectedSize = -1;
        long downloadedBytes = 0;

        String finalPath = Tools.removePrefix(pathKey);

        log.info("开始从 MinIO 下载文件, " + "bucket={}, object={}, target={}", bucketName, finalPath, absoluteTarget);

        try {

            /*
             * 先获取 Object 信息。
             *
             * 可以拿到 Content-Length，
             * 用于日志和最终校验。
             */
            expectedSize = amazonS3Client.getObjectMetadata(bucketName, finalPath).getContentLength();

            log.info("MinIO 文件大小, bucket={}, object={}, size={}", bucketName, finalPath, expectedSize);

            S3Object s3Object = amazonS3Client.getObject(bucketName, finalPath);

            try (S3ObjectInputStream s3InputStream = s3Object.getObjectContent();

                 InputStream inputStream = new BufferedInputStream(s3InputStream, BUFFER_SIZE);

                 OutputStream outputStream = new BufferedOutputStream(Files.newOutputStream(tempPath, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE), BUFFER_SIZE)) {

                byte[] buffer = new byte[BUFFER_SIZE];

                int read;

                long lastLogTime = System.currentTimeMillis();

                while ((read = inputStream.read(buffer)) != -1) {

                    outputStream.write(buffer, 0, read);

                    downloadedBytes += read;

                    /*
                     * 每 10 秒打印一次进度。
                     */
                    long now = System.currentTimeMillis();

                    if (now - lastLogTime >= 10_000) {

                        logProgress(finalPath, downloadedBytes, expectedSize, startTime);

                        lastLogTime = now;
                    }
                }

                /*
                 * 确保数据全部刷入磁盘。
                 */
                outputStream.flush();
            }

            /*
             * 下载完成后校验文件大小。
             */
            long actualSize = Files.size(tempPath);

            if (expectedSize >= 0 && actualSize != expectedSize) {

                throw new IOException("文件下载不完整, " + "expected=" + expectedSize + ", actual=" + actualSize + ", object=" + finalPath);
            }

            /*
             * 下载成功。
             *
             * 将临时文件移动成最终文件。
             */
            moveToTarget(tempPath, absoluteTarget);

            long cost = System.currentTimeMillis() - startTime;

            log.info("MinIO 文件下载完成, " + "bucket={}, object={}, target={}, " + "size={}, cost={}ms", bucketName, finalPath, absoluteTarget, actualSize, cost);

            return absoluteTarget;
        } catch (Exception e) {

            /*
             * 删除失败的临时文件。
             */
            try {
                Files.deleteIfExists(tempPath);
            } catch (Exception deleteException) {
                log.warn("删除下载临时文件失败, path={}", tempPath, deleteException);
            }

            if (e instanceof IOException) {
                throw (IOException) e;
            }

            throw new IOException("MinIO 文件下载失败, " + "bucket=" + bucketName + ", object=" + finalPath, e);
        }
    }


    /**
     * 将临时文件移动为最终文件。
     */
    private void moveToTarget(Path tempPath, Path targetPath) throws IOException {

        try {

            /*
             * 同一文件系统下通常可以原子移动。
             */
            Files.move(tempPath, targetPath, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);

        } catch (AtomicMoveNotSupportedException e) {

            /*
             * 某些文件系统不支持 ATOMIC_MOVE。
             *
             * 降级为普通移动。
             */
            log.debug("文件系统不支持 ATOMIC_MOVE, " + "降级为普通 MOVE, target={}", targetPath);

            Files.move(tempPath, targetPath, StandardCopyOption.REPLACE_EXISTING);
        }
    }


    /**
     * 下载进度日志。
     */
    private void logProgress(String objectName, long downloadedBytes, long expectedSize, long startTime) {

        double percentage = 0;

        if (expectedSize > 0) {
            percentage = downloadedBytes * 100.0 / expectedSize;
        }

        long elapsed = System.currentTimeMillis() - startTime;

        double speedMBps = 0;

        if (elapsed > 0) {

            speedMBps = downloadedBytes / 1024.0 / 1024.0 / (elapsed / 1000.0);
        }

        log.info("文件下载进度, " + "object={}, downloaded={} bytes, " + "total={} bytes, progress={}%, " + "speed={} MB/s", objectName, downloadedBytes, expectedSize, String.format("%.2f", percentage), String.format("%.2f", speedMBps));
    }
}
