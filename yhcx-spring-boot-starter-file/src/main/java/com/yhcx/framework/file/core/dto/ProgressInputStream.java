package com.yhcx.framework.file.core.dto;

import java.io.IOException;
import java.io.InputStream;

/**
 * 一个包装类，用于在读取 InputStream 时监控进度。
 */
public class ProgressInputStream extends InputStream {

    private final InputStream underlyingStream;
    private final long totalSize;
    private long bytesRead;
    private final ProgressListener listener;
    private int lastPercentReported = -1;

    public ProgressInputStream(InputStream underlyingStream, long totalSize, ProgressListener listener) {
        this.underlyingStream = underlyingStream;
        this.totalSize = totalSize;
        this.listener = listener;
        this.bytesRead = 0;
    }

    @Override
    public int read() throws IOException {
        int b = underlyingStream.read();
        if (b != -1) {
            bytesRead++;
            updateProgress();
        }
        return b;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        int bytes = underlyingStream.read(b, off, len);
        if (bytes != -1) {
            bytesRead += bytes;
            updateProgress();
        }
        return bytes;
    }

    private void updateProgress() {
        if (totalSize <= 0) {
            return; // 无法计算百分比
        }
        int percent = (int) (100 * bytesRead / totalSize);
        if (percent > lastPercentReported) {
            listener.onProgress(bytesRead, totalSize, percent);
            lastPercentReported = percent;
        }
    }

    @Override
    public void close() throws IOException {
        underlyingStream.close();
        // 确保最终100%被报告
        if (lastPercentReported < 100) {
            listener.onProgress(totalSize, totalSize, 100);
        }
    }

    // 定义一个进度监听器接口
    @FunctionalInterface
    public interface ProgressListener {
        void onProgress(long bytesTransferred, long totalBytes, int percent);
    }
}
