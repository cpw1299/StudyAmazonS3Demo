package com.yhcx.module.business.listener;

import com.yhcx.module.business.listener.event.DatasetSyncMaasEvnet;
import com.yhcx.module.business.service.bo.SourceTargetBO;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Component;

import java.util.List;

@Slf4j
@Component
public class DatasetFileSyncMaasListener implements ApplicationListener<DatasetSyncMaasEvnet> {

    @Override
    public void onApplicationEvent(DatasetSyncMaasEvnet event) {
        if (event.getBoList() == null) {
            log.warn("[同步Maas文件]业务数据为空");
            return;
        }
        List<SourceTargetBO> boList = event.getBoList();
        // 处理文件
    }

}
