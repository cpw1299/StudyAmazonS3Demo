package com.yhcx.module.business.service;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.yhcx.module.business.api.DatasetApi;
import com.yhcx.module.business.dal.dataobject.DatasetFileSyncRecordDO;
import com.yhcx.module.business.dal.dataobject.DatasetRecordInfoDO;
import com.yhcx.module.business.dal.mysql.DatasetRecordInfoMapper;
import com.yhcx.module.business.enums.YesOrNoEnum;
import com.yhcx.module.business.listener.event.DatasetSyncMaasEvnet;
import com.yhcx.module.business.service.bo.SourceTargetBO;
import com.yhcx.module.business.vo.DatasetSaveReqVo;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class DatasetSyncMaasServiceImpl implements DatasetSyncMaasService {

    private final DatasetRecordInfoMapper datasetRecordInfoMapper;

    private final DatasetApi datasetApi;

    private final ApplicationEventPublisher applicationEventPublisher;

    private final DatasetFileSyncRecordService recordService;

    private final DatasetSyncDataMaasService datasetSyncDataMaasService;

    // Maas数据库表dataset_record_info的status字段，只查询 PARSE_SUCCESS
    private final static String STATUS_PARSE_SUCCESS = "PARSE_SUCCESS";

    @Override
    public void copyDatasetRecordToDataset(List<Long> datasetRecordIds) {
        LambdaQueryWrapper<DatasetRecordInfoDO> queryWrapper = new LambdaQueryWrapper<>();
        queryWrapper.in(datasetRecordIds != null && !datasetRecordIds.isEmpty(), DatasetRecordInfoDO::getId, datasetRecordIds);
        queryWrapper.eq(DatasetRecordInfoDO::getDeleted, YesOrNoEnum.NO_0.getCode());
        List<DatasetRecordInfoDO> doList = datasetRecordInfoMapper.selectList(queryWrapper);
        if (doList == null || doList.isEmpty()) {
            log.warn("[同步Maas数据集]无数据");
            return;
        }
        // 已经存在同步记录的数据集直接复用原 targetDatasetId/targetRepositoryPath，
        // 避免重试时重新创建一个新数据集。
        Map<Long, DatasetFileSyncRecordDO> recordMap = doList.stream()
                .map(infoDO -> recordService.findBySourceDatasetId(infoDO.getId()))
                .filter(Objects::nonNull)
                .collect(Collectors.toMap(DatasetFileSyncRecordDO::getSourceDatasetId, t -> t, (v1, v2) -> v1));

        List<DatasetRecordInfoDO> needCreateList = doList.stream()
                .filter(infoDO -> !recordMap.containsKey(infoDO.getId()))
                .collect(Collectors.toList());

        List<DatasetSaveReqVo> savedList = needCreateList.isEmpty()
                ? new ArrayList<>()
                : datasetApi.createDataset(datasetSyncDataMaasService.convertData(needCreateList));
        Map<Long, DatasetSaveReqVo> savedMap = savedList.stream()
                .collect(Collectors.toMap(o -> Long.valueOf(o.getExtendDatasetId()), t -> t, (v1, v2) -> v1));

        List<SourceTargetBO> boList = new ArrayList<>();
        for (DatasetRecordInfoDO infoDO : doList) {
            if (!STATUS_PARSE_SUCCESS.equals(infoDO.getStatus())) {
                log.warn("[同步Maas数据集]状态不匹配，不执行同步文件！id={}，name={}，status={}",
                        infoDO.getId(), infoDO.getDatasetName(), infoDO.getStatus());
            }
            DatasetFileSyncRecordDO existingRecord = recordMap.get(infoDO.getId());
            if (existingRecord != null) {
                if (DatasetFileSyncRecordService.STATUS_SUCCESS.equals(existingRecord.getStatus())) {
                    log.info("[同步Maas数据集]文件已全部同步，跳过，sourceDatasetId={}, targetDatasetId={}",
                            infoDO.getId(), existingRecord.getTargetDatasetId());
                    continue;
                }
                DatasetSaveReqVo target = new DatasetSaveReqVo();
                target.setId(existingRecord.getTargetDatasetId());
                target.setRepositoryPath(existingRecord.getTargetRepositoryPath());
                target.setExtendDatasetId(String.valueOf(infoDO.getId()));
                boList.add(new SourceTargetBO(infoDO, target));
                continue;
            }

            DatasetSaveReqVo target = savedMap.get(infoDO.getId());
            if (target == null || target.getId() == null) {
                log.error("[同步Maas数据集]创建新数据集失败，跳过【{}】-【{}】", infoDO.getId(), infoDO.getDatasetName());
                continue;
            }
            recordService.getOrCreate(infoDO.getId(), target.getId(), target.getRepositoryPath());
            boList.add(new SourceTargetBO(infoDO, target));
        }
        if (boList.isEmpty()) {
            log.warn("[同步Maas数据集]没有需要同步的数据");
        } else {
            // 使用 ApplicationEvent 异步处理文件
            applicationEventPublisher.publishEvent(new DatasetSyncMaasEvnet(this, boList));
        }

        // 等待文件处理完毕后，通过事件告知业务模块，新数据集文件同步完成。
        // 禁止在此模块（yhcx-module-business-sync-data）调用新数据集的数据库查询。
        // 职责要单一，便于维护。
    }

}
