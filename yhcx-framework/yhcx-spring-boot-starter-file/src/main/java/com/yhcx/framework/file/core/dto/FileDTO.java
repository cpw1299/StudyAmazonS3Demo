package com.yhcx.framework.file.core.dto;


import io.swagger.v3.oas.annotations.media.Schema;
import java.util.Collections;
import java.util.Date;
import java.util.List;
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
public class FileDTO {

    /**
     * 文件数量
     */
    private Long fileCount;

    /**
     * 文件大小
     */
    private Long fileSize;

}
