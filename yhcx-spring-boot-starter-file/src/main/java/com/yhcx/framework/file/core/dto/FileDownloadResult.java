package com.yhcx.framework.file.core.dto;

import lombok.Data;

/**
 * FileDownloadResult
 *
 * @author liuhm
 * @version 1.0.0
 * @create 2025/12/1 9:50
 **/
@Data
public class FileDownloadResult {

    FileRespDTO file;
    byte[] content;
    Exception exception;

    public FileDownloadResult(FileRespDTO file, byte[] content, Exception exception) {
        this.file = file;
        this.content = content;
        this.exception = exception;
    }
}
