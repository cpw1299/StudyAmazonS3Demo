package com.yhcx.module.business.listener;

import com.yhcx.module.business.listener.event.DatasetSyncMaasEvnet;
import com.yhcx.module.business.service.DatasetFileSyncMaasService;
import com.yhcx.module.business.service.bo.SourceTargetBO;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Component;

import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class DatasetFileSyncMaasListener implements ApplicationListener<DatasetSyncMaasEvnet> {

    private final DatasetFileSyncMaasService datasetFileSyncMaasService;

    @Override
    public void onApplicationEvent(DatasetSyncMaasEvnet event) {
        List<SourceTargetBO> boList = event.getBoList();
        if (boList == null || boList.isEmpty()) {
            log.warn("[同步Maas文件]业务数据为空");
            return;
        }

        datasetFileSyncMaasService.copyDatasetFiles(boList);
    }

}
