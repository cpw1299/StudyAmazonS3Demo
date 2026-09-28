package com.yhcx.module.business.listener;

import com.yhcx.module.business.listener.event.DatasetSyncMaasEvnet;
import com.yhcx.module.business.service.DatasetFileSyncMaasService;
import com.yhcx.module.business.service.DatasetFileSyncRecordService;
import com.yhcx.module.business.service.bo.SourceTargetBO;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Async;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.List;

/**
 * Maas 数据集文件同步异步监听器。
 *
 * <p>事件发布线程只负责提交任务，真正的文件复制在 datasetSyncExecutor 线程池执行。</p>
 * <p>按数据集拆分任务，单个数据集失败不会影响同一事件中的其它数据集。</p>
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class DatasetFileSyncMaasListener implements ApplicationListener<DatasetSyncMaasEvnet> {

    private final DatasetFileSyncMaasService datasetFileSyncMaasService;
    private final DatasetFileSyncRecordService recordService;

    @Async("datasetSyncExecutor")
    @Override
    public void onApplicationEvent(DatasetSyncMaasEvnet event) {
        List<SourceTargetBO> boList = event.getBoList();
        if (boList == null || boList.isEmpty()) {
            log.warn("[同步Maas文件]业务数据为空");
            return;
        }

        for (SourceTargetBO bo : boList) {
            if (bo == null || bo.getSource() == null || bo.getTarget() == null) {
                log.error("[同步Maas文件]异步任务参数无效, bo={}", bo);
                continue;
            }

            try {
                // 按数据集独立执行，避免一个数据集失败导致整批任务中断。
                datasetFileSyncMaasService.copyDatasetFiles(Collections.singletonList(bo));
            } catch (Exception e) {
                markTaskFailed(bo, e);
            }
        }
    }

    private void markTaskFailed(SourceTargetBO bo, Exception e) {
        Long sourceDatasetId = bo.getSource().getId();
        Long targetDatasetId = bo.getTarget().getId();

        try {
            if (sourceDatasetId == null || targetDatasetId == null) {
                log.error("[同步Maas文件]任务失败且无法更新同步记录，sourceDatasetId={}, targetDatasetId={}",
                        sourceDatasetId, targetDatasetId, e);
                return;
            }

            // 文件服务内部已经会持久化异常；这里作为 Listener 最外层兜底，
            // 防止异步线程发生文件服务之外的异常后任务仍停留在 PROCESSING。
            recordService.markTaskFailed(
                    recordService.getOrCreate(
                            sourceDatasetId,
                            targetDatasetId,
                            bo.getTarget().getRepositoryPath()
                    ),
                    e
            );
        } catch (Exception recordException) {
            log.error("[同步Maas文件]更新失败状态失败，sourceDatasetId={}, targetDatasetId={}",
                    sourceDatasetId, targetDatasetId, recordException);
        }

        log.error("[同步Maas文件]异步任务执行失败，sourceDatasetId={}, targetDatasetId={}",
                sourceDatasetId, targetDatasetId, e);
    }
}
