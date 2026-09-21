package com.yhcx.module.business.framework;


import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.retry.PredefinedRetryPolicies;
import com.amazonaws.retry.RetryPolicy;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.services.s3.transfer.TransferManagerConfiguration;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.StringUtils;

@Slf4j
@Configuration
@EnableConfigurationProperties(AmazonS3Properties.class)
public class AmazonS3Config {

    private static final long MB = 1024L * 1024L;

    /**
     * S3 客户端。
     * 1. AmazonS3ClientBuilder.build() 返回的实例就是 AmazonS3Client（强转安全）；
     * 2. destroyMethod = "shutdown"：应用关闭时释放 HTTP 连接池，避免连接泄漏；
     * 3. 单例复用：AmazonS3Client 线程安全，切勿每次请求都 new 一个。
     */
    @Bean(destroyMethod = "shutdown")
    public AmazonS3Client amazonS3Client(AmazonS3Properties props) {

        // ---------- 1. 凭证 ----------
        AWSCredentialsProvider credentialsProvider = resolveCredentialsProvider(props);

        // ---------- 2. 客户端参数 ----------
        ClientConfiguration clientConfig = new ClientConfiguration();
        clientConfig.setProtocol(props.isUseHttps() ? Protocol.HTTPS : Protocol.HTTP);
        clientConfig.setMaxConnections(props.getMaxConnections());
        clientConfig.setConnectionTimeout(props.getConnectionTimeoutMs());
        clientConfig.setSocketTimeout(props.getSocketTimeoutMs());
        // 连接到期重建，防止 DNS 变更后一直连旧 IP（长生命周期 JVM 的经典坑）
        clientConfig.setConnectionTTL(props.getConnectionTtlMs());
        clientConfig.setTcpKeepAlive(props.isTcpKeepAlive());
        // 失败重试：默认条件 + 指数退避，对 5xx / 限流 / 瞬时网络抖动自动重试
        clientConfig.setMaxErrorRetry(props.getMaxErrorRetry());
        clientConfig.setRetryPolicy(new RetryPolicy(
                PredefinedRetryPolicies.DEFAULT_RETRY_CONDITION,
                PredefinedRetryPolicies.DEFAULT_BACKOFF_STRATEGY,
                props.getMaxErrorRetry(),
                true));

        // ---------- 3. 构建客户端 ----------
        AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withClientConfiguration(clientConfig)
                .withPathStyleAccessEnabled(props.isPathStyleAccess());

        if (!props.isChunkedEncodingEnabled()) {
            // 部分 S3 兼容存储（MinIO 旧版本等）不支持 chunked encoding 时关闭
            builder.disableChunkedEncoding();
        }

        if (StringUtils.hasText(props.getEndpoint())) {
            // 对接 MinIO / 私有化对象存储：显式指定 endpoint，签名区域用 region
            builder.setEndpointConfiguration(
                    new AwsClientBuilder.EndpointConfiguration(props.getEndpoint(), props.getRegion()));
        } else {
            // 标准 AWS：按 region 自动解析 endpoint
            builder.withRegion(props.getRegion());
        }

        AmazonS3Client client = (AmazonS3Client) builder.build();

        // 日志只打非敏感信息，严禁打印 AK/SK
        log.info("[S3] AmazonS3Client 初始化完成: endpoint={}, region={}, pathStyle={}, maxConnections={}",
                StringUtils.hasText(props.getEndpoint()) ? props.getEndpoint() : "aws-default",
                props.getRegion(), props.isPathStyleAccess(), props.getMaxConnections());
        return client;
    }

    /**
     * 大文件传输管理器：超过阈值自动分片并发上传，生产必备。
     */
    @Bean(destroyMethod = "shutdownNow")
    public TransferManager transferManager(AmazonS3Client amazonS3Client, AmazonS3Properties props) {
        TransferManagerConfiguration tmConfig = new TransferManagerConfiguration();
        tmConfig.setMultipartUploadThreshold(props.getMultipartThresholdMb() * MB);
        tmConfig.setMultipartUploadPartSize(props.getMultipartPartSizeMb() * MB);

        return TransferManagerBuilder.standard()
                .withS3Client(amazonS3Client)
                .withConfiguration(tmConfig)
                .build();
    }

    /**
     * 凭证策略：
     * - 配置了 AK/SK -> 静态凭证（自建环境 / MinIO）
     * - 未配置      -> 默认凭证链（AWS 上推荐：EC2 Instance Profile / ECS Task Role，免密钥管理）
     */
    private AWSCredentialsProvider resolveCredentialsProvider(AmazonS3Properties props) {
        if (StringUtils.hasText(props.getAccessKey()) && StringUtils.hasText(props.getSecretKey())) {
            return new AWSStaticCredentialsProvider(
                    new BasicAWSCredentials(props.getAccessKey(), props.getSecretKey()));
        }
        log.info("[S3] 未配置 AK/SK，使用 DefaultAWSCredentialsProviderChain（IAM Role）");
        return DefaultAWSCredentialsProviderChain.getInstance();
    }
}