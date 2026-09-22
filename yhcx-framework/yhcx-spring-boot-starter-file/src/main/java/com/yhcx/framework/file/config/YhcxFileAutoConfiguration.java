package com.yhcx.framework.file.config;

import com.yhcx.framework.file.core.service.*;
import com.yhcx.module.infra.framework.file.core.client.FileClientFactory;
import com.yhcx.module.infra.service.file.FileConfigService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.retry.annotation.EnableRetry;

/**
 * Webdav配置类
 *
 * @author 芋道源码
 */
@Slf4j
@EnableRetry
@Configuration
@EnableConfigurationProperties(YhcxFileProperty.class)
@ConditionalOnClass(value = {FileClientFactory.class})
public class YhcxFileAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public LocalFileStorageService localFileStorageService(YhcxFileProperty property) {
        return new LocalFileStorageServiceImpl(property);
    }

    @Bean
    public FileStoragePlusUltraService fileStoragePlusUltraService() {
        return new S3FileStoragePlusUltraServiceImpl();
    }

    @Bean
    public S3FileStorageService s3FileStorageService(FileConfigService fileConfigService,
                                                     S3TempFileDownloader s3TempFileDownloader,
                                                     ZipToMinioService zipToMinioService,
                                                     FileDownloadService fileDownloadService,
                                                     StringRedisTemplate stringRedisTemplate) {
        return new S3FileStorageServiceImpl(fileConfigService.getMasterFileClient(), s3TempFileDownloader,zipToMinioService, fileDownloadService, stringRedisTemplate);
    }

    @Bean
    public S3StreamDownloader s3StreamDownloader(FileConfigService fileConfigService) {
        return new S3StreamDownloader(fileConfigService);
    }

    @Bean
    public S3TempFileDownloader s3TempFileDownloader(FileConfigService fileConfigService) {
        return new S3TempFileDownloader(fileConfigService);
    }

    @Bean
    public ZipToMinioService zipToMinioService(FileConfigService fileConfigService) {
        return new ZipToMinioService(fileConfigService.getMasterFileClient());
    }

    @Bean
    public FileDownloadService minioFileDownloadService(FileConfigService fileConfigService) {
        return new FileDownloadService(fileConfigService.getMasterFileClient());
    }

}
