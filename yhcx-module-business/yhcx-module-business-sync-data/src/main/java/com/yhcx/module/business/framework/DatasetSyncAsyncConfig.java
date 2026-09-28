package com.yhcx.module.business.framework;

import lombok.extern.slf4j.Slf4j;
import org.springframework.aop.interceptor.AsyncUncaughtExceptionHandler;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.AsyncConfigurer;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.lang.reflect.Method;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadPoolExecutor;

@Slf4j
@Configuration
@EnableAsync
public class DatasetSyncAsyncConfig implements AsyncConfigurer {

    @Value("${dataset.sync.async.core-pool-size:4}")
    private int corePoolSize;

    @Value("${dataset.sync.async.max-pool-size:8}")
    private int maxPoolSize;

    @Value("${dataset.sync.async.queue-capacity:100}")
    private int queueCapacity;

    @Value("${dataset.sync.async.keep-alive-seconds:60}")
    private int keepAliveSeconds;

    @Value("${dataset.sync.async.await-termination-seconds:60}")
    private int awaitTerminationSeconds;

    @Bean("datasetSyncExecutor")
    public Executor datasetSyncExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(corePoolSize);
        executor.setMaxPoolSize(maxPoolSize);
        executor.setQueueCapacity(queueCapacity);
        executor.setKeepAliveSeconds(keepAliveSeconds);
        executor.setThreadNamePrefix("dataset-sync-");
        executor.setWaitForTasksToCompleteOnShutdown(true);
        executor.setAwaitTerminationSeconds(awaitTerminationSeconds);
        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.AbortPolicy());
        executor.initialize();
        return executor;
    }

    @Override
    public Executor getAsyncExecutor() {
        return datasetSyncExecutor();
    }

    @Override
    public AsyncUncaughtExceptionHandler getAsyncUncaughtExceptionHandler() {
        return new DatasetSyncAsyncExceptionHandler();
    }

    private static final class DatasetSyncAsyncExceptionHandler implements AsyncUncaughtExceptionHandler {

        @Override
        public void handleUncaughtException(Throwable ex, Method method, Object... params) {
            log.error("[同步Maas文件]异步任务未捕获异常, method={}", method.getName(), ex);
        }
    }
}
