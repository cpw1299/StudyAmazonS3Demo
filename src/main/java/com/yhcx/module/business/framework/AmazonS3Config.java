package com.yhcx.module.business.framework;


import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.client.builder.ExecutorFactory;
import com.amazonaws.retry.PredefinedRetryPolicies;
import com.amazonaws.retry.RetryPolicy;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.StringUtils;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@Configuration
@EnableConfigurationProperties(AmazonS3Properties.class)
public class AmazonS3Config {

    private static final long MB = 1024L * 1024L;

    /**
     * S3 客户端。
     *
     * 生产原则：
     * 1. AmazonS3Client 单例复用；SDK Client/连接池都是线程安全的；
     * 2. 应用关闭时执行 shutdown，释放 HTTP 连接池；
     * 3. MinIO 使用 endpoint + path-style + 合适的签名 region；
     * 4. AK/SK 不写死在代码中，优先通过 Secret/环境变量注入。
     */
    @Bean(destroyMethod = "shutdown")
    public AmazonS3Client amazonS3Client(AmazonS3Properties props) {

        // ---------- 1. 凭证 ----------
        AWSCredentialsProvider credentialsProvider = resolveCredentialsProvider(props);

        // ---------- 2. HTTP / SDK 客户端参数 ----------
        ClientConfiguration clientConfig = new ClientConfiguration();

        // 网络协议：MinIO 内网 HTTP 或 HTTPS 均可；生产环境优先 HTTPS。
        clientConfig.setProtocol(props.isUseHttps() ? Protocol.HTTPS : Protocol.HTTP);

        // HTTP 连接池最大连接数。应覆盖业务并发请求 + TransferManager 分片并发。
        clientConfig.setMaxConnections(props.getMaxConnections());

        // TCP 建连超时：只控制建立 TCP/TLS 连接阶段，不控制对象传输时间。
        clientConfig.setConnectionTimeout(props.getConnectionTimeoutMs());

        // Socket 读写超时：单次网络读写长期没有数据时超时。
        // 大文件上传建议不要设置过小，否则网络抖动会导致分片频繁失败重试。
        clientConfig.setSocketTimeout(props.getSocketTimeoutMs());

        // HTTP 连接最大空闲时间：防止服务端/LB 已经关闭空闲连接，而客户端继续复用。
        clientConfig.setConnectionMaxIdleMillis(props.getConnectionMaxIdleMs());

        // 连接池中的连接经过该时间未使用后，重新校验是否仍然可用。
        clientConfig.setValidateAfterInactivityMillis(props.getValidateAfterInactivityMs());

        // 连接 TTL：连接建立超过该时间后不再复用，适合服务端/LB/DNS 有变更的生产环境。
        clientConfig.setConnectionTTL(props.getConnectionTtlMs());

        // AWS SDK 1.x 正确的方法名是 setUseTcpKeepAlive，而不是 setTcpKeepAlive。
        // 开启后允许 TCP 层发送 keepalive，减少长连接被网络设备静默回收的问题。
        clientConfig.setUseTcpKeepAlive(props.isTcpKeepAlive());

        // SDK 请求级最大重试次数。
        clientConfig.setMaxErrorRetry(props.getMaxErrorRetry());

        // 使用 SDK 默认重试条件 + 指数退避。
        clientConfig.setRetryPolicy(new RetryPolicy(
                PredefinedRetryPolicies.DEFAULT_RETRY_CONDITION,
                PredefinedRetryPolicies.DEFAULT_BACKOFF_STRATEGY,
                props.getMaxErrorRetry(),
                true));

        // ---------- 3. 构建 S3 Client ----------
        AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withClientConfiguration(clientConfig)
                // MinIO 推荐 path-style：/bucket/object，而不是 bucket.endpoint/object。
                .withPathStyleAccessEnabled(props.isPathStyleAccess());

        // 某些 S3 兼容服务对 AWS SDK 的 chunked encoding 支持不完整。
        // MinIO 场景通常建议关闭，尤其是经过代理/LB 时。
        if (!props.isChunkedEncodingEnabled()) {
            builder.disableChunkedEncoding();
        }

        if (StringUtils.hasText(props.getEndpoint())) {
            // MinIO / 私有化 S3：endpoint 是实际访问地址，region 用于签名。
            builder.setEndpointConfiguration(
                    new AwsClientBuilder.EndpointConfiguration(
                            props.getEndpoint(),
                            props.getRegion()));
        } else {
            // AWS S3：根据 region 自动选择官方 endpoint。
            builder.withRegion(props.getRegion());
        }

        AmazonS3Client client = (AmazonS3Client) builder.build();

        log.info(
                "[S3] AmazonS3Client 初始化完成: endpoint={}, region={}, pathStyle={}, maxConnections={}, socketTimeoutMs={}",
                StringUtils.hasText(props.getEndpoint()) ? props.getEndpoint() : "aws-default",
                props.getRegion(),
                props.isPathStyleAccess(),
                props.getMaxConnections(),
                props.getSocketTimeoutMs());

        return client;
    }

    /**
     * TransferManager。
     *
     * AWS SDK 1.12.797 中：
     * - 不再使用不存在的 setMultipartUploadPartSize(...)
     * - 不再使用不存在的 withConfiguration(...)
     * - 应直接使用 TransferManagerBuilder.withMinimumUploadPartSize(...)
     *   和 withMultipartUploadThreshold(...)
     */
    @Bean(destroyMethod = "shutdownNow")
    public TransferManager transferManager(AmazonS3Client amazonS3Client, AmazonS3Properties props) {

        ExecutorFactory executorFactory = () -> {
            ThreadFactory threadFactory = new ThreadFactory() {
                private final AtomicInteger sequence = new AtomicInteger(1);

                @Override
                public Thread newThread(Runnable runnable) {
                    Thread thread = new Thread(runnable, "s3-transfer-" + sequence.getAndIncrement());
                    thread.setDaemon(true);
                    return thread;
                }
            };

            // AWS 官方文档不建议 TransferManager 使用有界队列，
            // 因为控制任务可能再提交子任务，有界队列配置不当可能造成死锁。
            ExecutorService executor = Executors.newFixedThreadPool(
                    props.getTransferThreads(),
                    threadFactory);
            return executor;
        };

        return TransferManagerBuilder.standard()
                .withS3Client(amazonS3Client)

                // 超过此大小才启用 multipart upload。
                .withMultipartUploadThreshold(props.getMultipartThresholdMb() * MB)

                // AWS SDK 1.12.797 的正确 API：
                // minimum upload part size，单位为 byte。
                .withMinimumUploadPartSize(props.getMultipartPartSizeMb() * MB)

                // 大文件下载也可并行；如果业务只需要上传，可保持默认 false。
                .withDisableParallelDownloads(props.isDisableParallelDownloads())

                // 对 MinIO/Object Lock 等场景可按需开启 multipart MD5。
                .withAlwaysCalculateMultipartMd5(props.isAlwaysCalculateMultipartMd5())

                // 自定义 TransferManager 工作线程数。
                .withExecutorFactory(executorFactory)

                .build();
    }

    /**
     * 凭证策略：
     * - 配置 AK/SK -> 静态凭证，适合 MinIO；
     * - 未配置     -> DefaultAWSCredentialsProviderChain，AWS 生产环境推荐 IAM Role。
     */
    private AWSCredentialsProvider resolveCredentialsProvider(AmazonS3Properties props) {
        if (StringUtils.hasText(props.getAccessKey()) && StringUtils.hasText(props.getSecretKey())) {
            return new AWSStaticCredentialsProvider(
                    new BasicAWSCredentials(props.getAccessKey(), props.getSecretKey()));
        }

        log.info("[S3] 未配置 AK/SK，使用 DefaultAWSCredentialsProviderChain（推荐 IAM Role）");
        return DefaultAWSCredentialsProviderChain.getInstance();
    }
}
