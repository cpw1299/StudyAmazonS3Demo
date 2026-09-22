package com.yhcx.framework.file.core.dto;


import io.swagger.v3.oas.annotations.media.Schema;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 文件目录 响应对象
 *
 * @author liuhm
 * @version 1.0.0
 * @create 2025/4/7 11:05
 **/
@Schema(description = "文件目录 响应对象")
@Data
@NoArgsConstructor
@AllArgsConstructor
public class FileUploadDTO {

    /**
     * 文件路径（包含目录）
     */
    private String filePath;

    /**
     * 文件名称
     */
    private String fileName;

    /**
     * MimeType
     */
    private String mimeType;

    /**
     * 文件内容
     */
    private byte[] content;

    /**
     * 文件大小
     */
    private long size;

}
