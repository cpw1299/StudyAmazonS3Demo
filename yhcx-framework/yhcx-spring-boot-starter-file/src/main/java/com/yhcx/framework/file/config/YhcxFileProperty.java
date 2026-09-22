package com.yhcx.framework.file.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 本地文件存储配置
 */
@Data
@ConfigurationProperties(prefix = "yhcx.file.local")
public class YhcxFileProperty {

    /** 文件最终上传目录 */
    private String uploadPath = "./data/files";

    /** 分片临时存储目录 */
    private String partPath = "./data/file-parts";
}
