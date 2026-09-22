package com.yhcx.framework.file.core.dto;


import io.swagger.v3.oas.annotations.media.Schema;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 文件进度
 *
 * @author liuhm
 * @version 1.0.0
 * @create 2025/4/7 11:05
 **/
@Schema(description = "文件进度")
@Data
@NoArgsConstructor
@AllArgsConstructor
public class FilePercentDTO {

    private String uid;

    /**
     * 文件名称
     */
    private String name;

    /**
     * 进度
     */
    private Integer percent;

}
