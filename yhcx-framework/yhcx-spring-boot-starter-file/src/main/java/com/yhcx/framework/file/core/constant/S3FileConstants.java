package com.yhcx.framework.file.core.constant;

/**
 * S3/MinIO 文件存储相关业务常量
 *
 * @author liuhm
 * @version 1.0.0
 * @create 2025/4/8 16:22
 */
public interface S3FileConstants {

    /** MinIO底层元数据存储key：小写datasetid */
    String STORE_DATASET_ID = "datasetid";

    /** 前端响应交互key：驼峰datasetId */
    String FRONT_DATASET_ID = "datasetId";

}
