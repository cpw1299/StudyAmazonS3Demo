package com.yhcx.framework.file.core.dto;


import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Data;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * 文件目录 响应对象
 *
 * @author liuhm
 * @version 1.0.0
 * @create 2025/4/7 11:05
 **/
@Schema(description = "文件目录 响应对象")
@Data
public class FileRespDTO {

    /**
     * 文件原始名称
     */
    private String name;

    /**
     * 文件显示名称
     */
    private String displayName;

    /**
     * 文件大小
     */
    private Long size;

    /**
     * 文件大小格式化
     */
    private String sizeFormatted;

    /**
     * 文件后缀,不包含.
     */
    private String fileSuffix;

    /**
     * 创建时间
     */
    private Date createTime;

    /**
     * 相对/dav/dataset/ 的路径
     */
    private String path;

    /**
     * 原始路径
     */
    private String originalPath;

    /**
     * md5
     */
    private String md5sum;

    /**
     * 是否是文件夹
     */
    private Boolean folder;

    /**
     * 是否是文件夹
     */
    private Map<String, Object> metadata;

    public void putMetadata(String k, Object v) {
        if (this.metadata == null) {
            this.metadata = new HashMap<>();
        }
        this.metadata.put(k, v);
    }

}
