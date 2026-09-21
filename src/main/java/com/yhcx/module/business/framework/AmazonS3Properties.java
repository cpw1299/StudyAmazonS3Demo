package com.yhcx.module.business.framework;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * S3 / MinIO 客户端配置。
 *
 * 说明：
 * 1. 默认值按中等生产负载设计，不代表所有机器都应使用同一数值；
 * 2. 高并发、大文件场景应结合 CPU、带宽、MinIO 节点数和连接数压测；
 * 3. AK/SK 不建议写入 application.yml，生产使用 K8s Secret / Vault / 环境变量。
 */
@Data
@ConfigurationProperties(prefix = "aws.s3")
public class AmazonS3Properties {

    /** S3 签名 Region；MinIO 可使用服务端配置的 region，常见为 us-east-1。 */
    private String region = "us-east-1";

    /** MinIO / 私有化 S3 endpoint；例如 https://minio.example.com:9000。 */
    private String endpoint;

    /** Access Key；生产环境建议由 Secret/环境变量注入。 */
    private String accessKey;

    /** Secret Key；生产环境建议由 Secret/环境变量注入。 */
    private String secretKey;

    /** 默认 bucket；建议由 IaC/运维预先创建，应用只负责使用。 */
    private String defaultBucket;

    /** 是否使用 path-style：/bucket/object。MinIO 生产环境通常开启。 */
    private boolean pathStyleAccess = true;

    /** 是否启用 HTTP chunked encoding；兼容性优先时 MinIO 建议关闭。 */
    private boolean chunkedEncodingEnabled = false;

    /** HTTP 连接池最大连接数；应 >= TransferManager 并发 + 普通 S3 请求并发。 */
    private int maxConnections = 200;

    /** TCP/TLS 建连超时，毫秒。 */
    private int connectionTimeoutMs = 5000;

    /**
     * Socket 读写超时，毫秒。
     * 大文件/慢网络不能过小，否则一个正常分片可能因短暂无数据被判定超时。
     */
    private int socketTimeoutMs = 120000;

    /** HTTP 连接最大空闲时间，毫秒；降低复用已被 LB/MinIO 关闭的旧连接的概率。 */
    private long connectionMaxIdleMs = 60000;

    /** 连接空闲超过该值后，在从连接池取出时进行可用性校验，毫秒。 */
    private int validateAfterInactivityMs = 5000;

    /** 单条 HTTP 连接最大生命周期，毫秒；到期后重新建立连接。 */
    private long connectionTtlMs = 300000;

    /** 单个请求最多自动重试次数；需结合业务幂等性与 MinIO 限流策略调整。 */
    private int maxErrorRetry = 3;

    /** TCP KeepAlive；长连接生产环境建议开启。 */
    private boolean tcpKeepAlive = true;

    /** 是否使用 HTTPS；生产环境建议开启。 */
    private boolean useHttps = true;

    /** TransferManager 工作线程数；控制 multipart 分片上传/下载并发。 */
    private int transferThreads = 16;

    /** 超过该大小才启用 multipart upload，单位 MB。 */
    private long multipartThresholdMb = 32;

    /** multipart 每个分片的最小大小，单位 MB；最终会换算为 byte。 */
    private long multipartPartSizeMb = 16;

    /** 是否禁用 TransferManager 的并行下载；false 表示允许并行下载。 */
    private boolean disableParallelDownloads = false;

    /**
     * 是否强制为 multipart upload 计算 MD5。
     * Object Lock 等场景可能需要；普通 MinIO 上传一般不需要。
     */
    private boolean alwaysCalculateMultipartMd5 = false;

    /** 是否在启动时自动创建 bucket；生产建议 false，由 IaC/运维创建。 */
    private boolean autoCreateBucket = false;
}
