package com.yhcx.module.business.framework;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * S3 配置属性
 * 注意：Spring Boot 3.x 中 javax.validation 换成 jakarta.validation
 */
@Data
@ConfigurationProperties(prefix = "aws.s3")
public class AmazonS3Properties {

    /**
     * 区域，如 us-east-1、cn-north-1
     */
    private String region = "cn-east-1";

    /**
     * 自定义 endpoint，兼容 MinIO / 私有化 S3；标准 AWS 留空
     */
    private String endpoint;

    /**
     * 访问密钥，留空则走默认凭证链
     */
    private String accessKey;

    /**
     * 私有密钥
     */
    private String secretKey;

    /**
     * 默认操作的 bucket
     */
    private String defaultBucket;

    /**
     * path-style 访问，MinIO 等需开启
     */
    private boolean pathStyleAccess = false;

    /**
     * 是否启用 chunked encoding，部分 S3 兼容存储不支持，需关闭
     */
    private boolean chunkedEncodingEnabled = true;

    /**
     * 最大连接数
     */
    private int maxConnections = 200;

    /**
     * 建连超时（毫秒）
     */
    private int connectionTimeoutMs = 5_000;

    /**
     * 数据读取超时（毫秒），必须设置，否则可能线程挂死
     */
    private int socketTimeoutMs = 30_000;

    /**
     * 连接存活时间（毫秒），到期重建连接以刷新 DNS
     */
    private int connectionTtlMs = 60_000;

    /**
     * 请求失败自动重试次数
     */
    private int maxErrorRetry = 3;

    /**
     * TCP keep-alive
     */
    private boolean tcpKeepAlive = true;

    /**
     * 是否使用 HTTPS
     */
    private boolean useHttps = true;

    /**
     * 超过此大小（MB）自动分片上传
     */
    private long multipartThresholdMb = 32;

    /**
     * 分片大小（MB）
     */
    private long multipartPartSizeMb = 32;

    /**
     * 启动时 bucket 不存在是否自动创建（AWS 生产建议关闭，用 IaC 管理）
     */
    private boolean autoCreateBucket = false;
}
