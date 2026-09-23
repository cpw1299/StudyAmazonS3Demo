package com.yhcx.module.business.framework;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * AWS S3 / MinIO 配置。
 *
 * AWS SDK 2.x：
 * - 普通对象操作使用 S3Client；
 * - 文件传输使用 S3TransferManager + AWS CRT S3AsyncClient；
 * - 800GB 级对象必须重点关注 multipart part size、并发数和网络带宽。
 */
@Data
@ConfigurationProperties(prefix = "yhcx.aws.s3")
public class AmazonS3Properties {

    /** S3 签名 Region；MinIO 未特别配置时通常使用 us-east-1。 */
    private String region = "us-east-1";

    /** MinIO / 私有化 S3 endpoint，例如 https://minio.example.com:9000。 */
    private String endpoint;

    /** Access Key；生产环境建议由 K8s Secret / Vault / 环境变量注入。 */
    private String accessKey;

    /** Secret Key；生产环境建议由 K8s Secret / Vault / 环境变量注入。 */
    private String secretKey;

    /** 默认 bucket；生产建议由 IaC/运维提前创建。 */
    private String defaultBucket;

    /** MinIO 通常开启 path-style：/bucket/object。 */
    private boolean pathStyleAccess = true;

    /** 是否启用 HTTPS；生产环境建议 true。 */
    private boolean useHttps = true;

    /** S3 API 最大连接并发；普通 S3Client 与 TransferManager 共用此类网络资源时需要压测。 */
    private int maxConnections = 200;

    /** TCP/TLS 建连超时，毫秒。 */
    private long connectionTimeoutMs = 5000;

    /** 单次 API 调用尝试的超时时间，毫秒；大文件分片不应设置得过小。 */
    private long apiCallAttemptTimeoutMs = 120000;

    /** 单次 API 调用总超时时间，毫秒；应大于 attempt timeout。 */
    private long apiCallTimeoutMs = 180000;

    /** SDK 标准客户端最大重试次数。 */
    private int maxRetries = 3;

    /**
     * multipart 触发阈值，单位 MB。
     * 800GB 文件远高于该值，会自动进入 multipart。
     */
    private long multipartThresholdMb = 128;

    /**
     * multipart 最小 part 大小，单位 MB。
     *
     * 800GB 场景建议至少 128MB 级别，否则容易超过 S3 multipart 最大 10,000 parts 限制。
     */
    private long multipartPartSizeMb = 128;

    /**
     * CRT S3 最大并发连接数。
     * 实际吞吐还受机器 CPU、内存、网卡、MinIO 集群和 LB 限制。
     */
    private int maxConcurrency = 16;

    /**
     * CRT 目标吞吐，Gbps。
     * 仅作为 CRT 调优目标，不代表一定能达到。
     */
    private double targetThroughputGbps = 5.0;

    /** 是否启动时自动创建 bucket；生产建议 false。 */
    private boolean autoCreateBucket = false;
}
