package com.yhcx.module.business.framework;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * dataset_record_info 文件复制配置。
 */
@Data
@ConfigurationProperties(prefix = "dataset.copy")
public class DatasetCopyProperties {

    /** 目标 MinIO bucket，固定值通过配置注入。 */
    private String targetBucket;
}
