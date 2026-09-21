package com.yhcx.module.business.listener.event;

import com.yhcx.module.business.service.bo.SourceTargetBO;
import lombok.Getter;
import lombok.Setter;
import org.springframework.context.ApplicationEvent;

import java.time.Clock;
import java.util.List;

@Getter
@Setter
public class DatasetSyncMaasEvnet extends ApplicationEvent {

    private List<SourceTargetBO> boList;

    public DatasetSyncMaasEvnet(Object source, List<SourceTargetBO> boList) {
        super(source);
        this.boList = boList;
    }

    public DatasetSyncMaasEvnet(Object source, Clock clock, List<SourceTargetBO> boList) {
        super(source, clock);
        this.boList = boList;
    }
}
