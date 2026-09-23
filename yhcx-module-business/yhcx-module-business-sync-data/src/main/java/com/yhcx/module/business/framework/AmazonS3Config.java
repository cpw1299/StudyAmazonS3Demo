package com.yhcx.module.business.framework;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.StringUtils;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.retries.DefaultRetryStrategy;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.http.apache5.Apache5HttpClient;

import java.net.URI;
import java.time.Duration;

/**
 * AWS SDK for Java 2.x S3 配置。
 *
 * 设计：
 * 1. S3Client：普通同步对象操作；
 * 2. S3Presigner：预签名 URL；
 * 3. S3AsyncClient(CRT)：大文件、高吞吐、multipart；
 * 4. S3TransferManager：文件上传/下载的高层封装。
 *
 * 800GB 文件后续会基于 S3TransferManager + CRT 做专门的大文件上传能力。
 */
@Slf4j
@Configuration
@EnableConfigurationProperties({AmazonS3Properties.class})
public class AmazonS3Config {

    private static final long MB = 1024L * 1024L;

    /**
     * 普通 S3 同步客户端。
     *
     * 单例复用，Spring 关闭时自动 close。
     */
    @Bean(destroyMethod = "close")
    public S3Client s3Client(AmazonS3Properties props) {
        AwsCredentialsProvider credentialsProvider = resolveCredentialsProvider(props);

        Apache5HttpClient.Builder httpClientBuilder = Apache5HttpClient.builder()
                .maxConnections(props.getMaxConnections())
                .connectionTimeout(Duration.ofMillis(props.getConnectionTimeoutMs()));

        ClientOverrideConfiguration overrideConfiguration = ClientOverrideConfiguration.builder()
                .apiCallAttemptTimeout(Duration.ofMillis(props.getApiCallAttemptTimeoutMs()))
                .apiCallTimeout(Duration.ofMillis(props.getApiCallTimeoutMs()))
                .retryStrategy(DefaultRetryStrategy.standardStrategyBuilder()
                        .maxAttempts(props.getMaxRetries() + 1)
                        .build())
                .build();

        software.amazon.awssdk.services.s3.S3ClientBuilder builder = S3Client.builder()
                .region(Region.of(props.getRegion()))
                .credentialsProvider(credentialsProvider)
                .forcePathStyle(props.isPathStyleAccess())
                .httpClientBuilder(httpClientBuilder)
                .overrideConfiguration(overrideConfiguration);

        if (StringUtils.hasText(props.getEndpoint())) {
            builder.endpointOverride(URI.create(props.getEndpoint()));
        }

        S3Client client = builder.build();

        log.info(
                "[S3] S3Client(v2) 初始化完成: endpoint={}, region={}, pathStyle={}, maxConnections={}",
                StringUtils.hasText(props.getEndpoint()) ? props.getEndpoint() : "aws-default",
                props.getRegion(),
                props.isPathStyleAccess(),
                props.getMaxConnections());

        return client;
    }

    /**
     * AWS CRT-based S3 异步客户端。
     *
     * 这是后续 800GB 大文件上传的核心客户端：
     * - 自动 multipart；
     * - 分片并行；
     * - 单个失败 part 可单独重试；
     * - 更适合高吞吐、大对象。
     */
    @Bean(destroyMethod = "close")
    public S3AsyncClient s3AsyncClient(AmazonS3Properties props) {
        AwsCredentialsProvider credentialsProvider = resolveCredentialsProvider(props);

        software.amazon.awssdk.services.s3.S3CrtAsyncClientBuilder builder =
                S3AsyncClient.crtBuilder()
                        .region(Region.of(props.getRegion()))
                        .credentialsProvider(credentialsProvider)
                        .forcePathStyle(props.isPathStyleAccess())
                        .minimumPartSizeInBytes(props.getMultipartPartSizeMb() * MB)
                        .thresholdInBytes(props.getMultipartThresholdMb() * MB)
                        .maxConcurrency(props.getMaxConcurrency())
                        .targetThroughputInGbps(props.getTargetThroughputGbps());

        if (StringUtils.hasText(props.getEndpoint())) {
            builder.endpointOverride(URI.create(props.getEndpoint()));
        }

        S3AsyncClient client = builder.build();

        log.info(
                "[S3] S3AsyncClient(CRT) 初始化完成: endpoint={}, region={}, pathStyle={}, partSizeMb={}, thresholdMb={}, maxConcurrency={}, targetThroughputGbps={}",
                StringUtils.hasText(props.getEndpoint()) ? props.getEndpoint() : "aws-default",
                props.getRegion(),
                props.isPathStyleAccess(),
                props.getMultipartPartSizeMb(),
                props.getMultipartThresholdMb(),
                props.getMaxConcurrency(),
                props.getTargetThroughputGbps());

        return client;
    }

    /**
     * S3 Transfer Manager v2。
     *
     * v2 已从 v1 的同步 TransferManager 改为异步 S3TransferManager。
     * 真正的 multipart 参数由 S3AsyncClient(CRT) 提供。
     */
    @Bean(destroyMethod = "close")
    public S3TransferManager s3TransferManager(S3AsyncClient s3AsyncClient) {
        return S3TransferManager.builder()
                .s3Client(s3AsyncClient)
                .build();
    }

    /**
     * 预签名 URL 客户端。
     */
    @Bean(destroyMethod = "close")
    public S3Presigner s3Presigner(AmazonS3Properties props) {
        AwsCredentialsProvider credentialsProvider = resolveCredentialsProvider(props);

        S3Presigner.Builder builder = S3Presigner.builder()
                .region(Region.of(props.getRegion()))
                .credentialsProvider(credentialsProvider)
                .serviceConfiguration(S3Configuration.builder()
                        .pathStyleAccessEnabled(props.isPathStyleAccess())
                        .build());

        if (StringUtils.hasText(props.getEndpoint())) {
            builder.endpointOverride(URI.create(props.getEndpoint()));
        }

        return builder.build();
    }

    private AwsCredentialsProvider resolveCredentialsProvider(AmazonS3Properties props) {
        if (StringUtils.hasText(props.getAccessKey())
                && StringUtils.hasText(props.getSecretKey())) {
            return StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(props.getAccessKey(), props.getSecretKey()));
        }

        log.info("[S3] 未配置 AK/SK，使用 DefaultCredentialsProvider（AWS 生产环境可使用 IAM Role）");
        return DefaultCredentialsProvider.create();
    }
}
