package com.yhcx.module.business.service;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.yhcx.module.business.api.DatasetApi;
import com.yhcx.module.business.dal.dataobject.DatasetRecordInfoDO;
import com.yhcx.module.business.dal.mysql.DatasetRecordInfoMapper;
import com.yhcx.module.business.listener.event.DatasetSyncMaasEvnet;
import com.yhcx.module.business.service.bo.SourceTargetBO;
import com.yhcx.module.business.dal.dataobject.DatasetFileSyncRecordDO;
import com.yhcx.module.business.vo.DatasetMaasSaveReqVO;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class DatasetSyncMaasServiceImpl implements DatasetSyncMaasService {

    private final DatasetRecordInfoMapper datasetRecordInfoMapper;

    private final DatasetApi datasetApi;

    private final ApplicationEventPublisher applicationEventPublisher;

    private final DatasetFileSyncRecordService recordService;

    @Override
    public void copyDatasetRecordToDataset(List<Long> datasetRecordIds) {
        LambdaQueryWrapper<DatasetRecordInfoDO> queryWrapper = new LambdaQueryWrapper<>();
        queryWrapper.in(datasetRecordIds != null && !datasetRecordIds.isEmpty(), DatasetRecordInfoDO::getId, datasetRecordIds);
        List<DatasetRecordInfoDO> doList = datasetRecordInfoMapper.selectList(queryWrapper);
        if (doList == null || doList.isEmpty()) {
            log.warn("[同步Maas数据集]无数据");
            return;
        }
        // 已经存在同步记录的数据集直接复用原 targetDatasetId/targetRepositoryPath，
        // 避免重试时重新创建一个新数据集。
        Map<Long, DatasetFileSyncRecordDO> recordMap = doList.stream()
                .map(infoDO -> recordService.findBySourceDatasetId(infoDO.getId()))
                .filter(record -> record != null)
                .collect(Collectors.toMap(DatasetFileSyncRecordDO::getSourceDatasetId, t -> t, (v1, v2) -> v1));

        List<DatasetRecordInfoDO> needCreateList = doList.stream()
                .filter(infoDO -> !recordMap.containsKey(infoDO.getId()))
                .collect(Collectors.toList());

        List<DatasetMaasSaveReqVO> savedList = needCreateList.isEmpty()
                ? new ArrayList<>()
                : datasetApi.createDataset(convertData(needCreateList));
        Map<Long, DatasetMaasSaveReqVO> savedMap = savedList.stream()
                .collect(Collectors.toMap(DatasetMaasSaveReqVO::getSourceId, t -> t, (v1, v2) -> v1));

        List<SourceTargetBO> boList = new ArrayList<>();
        for (DatasetRecordInfoDO infoDO : doList) {
            DatasetFileSyncRecordDO existingRecord = recordMap.get(infoDO.getId());
            if (existingRecord != null) {
                if (DatasetFileSyncRecordService.STATUS_SUCCESS.equals(existingRecord.getStatus())) {
                    log.info("[同步Maas数据集]文件已全部同步，跳过，sourceDatasetId={}, targetDatasetId={}",
                            infoDO.getId(), existingRecord.getTargetDatasetId());
                    continue;
                }
                DatasetMaasSaveReqVO target = new DatasetMaasSaveReqVO();
                target.setId(existingRecord.getTargetDatasetId());
                target.setRepositoryPath(existingRecord.getTargetRepositoryPath());
                target.setSourceId(infoDO.getId());
                boList.add(new SourceTargetBO(infoDO, target));
                continue;
            }

            DatasetMaasSaveReqVO target = savedMap.get(infoDO.getId());
            if (target == null || target.getId() == null) {
                throw new IllegalStateException("创建新数据集失败，sourceDatasetId=" + infoDO.getId());
            }
            recordService.getOrCreate(infoDO.getId(), target.getId(), target.getRepositoryPath());
            boList.add(new SourceTargetBO(infoDO, target));
        }

        // 使用 ApplicationEvent 异步处理文件
        applicationEventPublisher.publishEvent(
                new DatasetSyncMaasEvnet(this, boList)
        );

        // 等待文件处理完毕后，通过事件告知业务模块，新数据集文件同步完成。
        // 禁止在此模块（yhcx-module-business-sync-data）调用新数据集的数据库查询。
        // 职责要单一，便于维护。
    }

    private List<DatasetMaasSaveReqVO> convertData(List<DatasetRecordInfoDO> doList) {
        List<DatasetMaasSaveReqVO> resultList = new ArrayList<>();
        // TODO 待转换
        for (DatasetRecordInfoDO infoDO : doList) {
            DatasetMaasSaveReqVO reqVO = new DatasetMaasSaveReqVO();
            reqVO.setId(5L);
            reqVO.setClientId("");
            reqVO.setStorageDir("");
            reqVO.setCompanyId("2");
            reqVO.setTeamId("3");
            reqVO.setParentId(0L);
            reqVO.setVersionCode("");
            reqVO.setProjectId(0L);
            reqVO.setEmqFileTemplateId(0L);
            reqVO.setEmqTableTemplateId(0L);
            reqVO.setDataStructuredModals("");
            reqVO.setSceneId(0L);
            reqVO.setPartId(0L);
            reqVO.setPartIds("");
            reqVO.setLicense("");
            reqVO.setOwner("");
            reqVO.setIndustryType(0);
            reqVO.setResourceId(0L);
            reqVO.setResourceLifecycleId(0L);
            reqVO.setPublicType(0);
            reqVO.setUniqueCode("");
            reqVO.setName("");
            reqVO.setLabelFlag(0);
            reqVO.setLabelType(0L);
            reqVO.setDataType(0);
            reqVO.setExtentSceneType(0);
            reqVO.setDatasetType(0);
            reqVO.setDatasetDate(LocalDateTime.now());
            reqVO.setDatasetStatus(0);
            reqVO.setDatasetReleaseType(0);
            reqVO.setShareStatus(0);
            reqVO.setRepositoryId("");
            reqVO.setRepositoryPath("/dataset/10/2/3/9-18标注");
            reqVO.setInstructions("");
            reqVO.setDatasetMeta("");
            reqVO.setLabelsMeta("");
            reqVO.setFieldsDesc("");
            reqVO.setSourceId(infoDO.getId());

            resultList.add(reqVO);
        }
        return resultList;
    }

}
