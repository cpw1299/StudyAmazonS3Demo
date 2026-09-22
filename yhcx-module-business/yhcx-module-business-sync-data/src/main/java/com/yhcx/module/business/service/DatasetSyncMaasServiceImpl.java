package com.yhcx.module.business.service;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.yhcx.module.business.api.DatasetApi;
import com.yhcx.module.business.vo.DatasetMaasSaveReqVO;
import com.yhcx.module.business.dal.dataobject.DatasetRecordInfoDO;
import com.yhcx.module.business.dal.mysql.DatasetRecordInfoMapper;
import com.yhcx.module.business.listener.event.DatasetSyncMaasEvnet;
import com.yhcx.module.business.service.bo.SourceTargetBO;
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

    @Override
    public void copyDatasetRecordToDataset(List<Long> datasetRecordIds) {
        LambdaQueryWrapper<DatasetRecordInfoDO> queryWrapper = new LambdaQueryWrapper<>();
        queryWrapper.in(datasetRecordIds != null && !datasetRecordIds.isEmpty(), DatasetRecordInfoDO::getId, datasetRecordIds);
        List<DatasetRecordInfoDO> doList = datasetRecordInfoMapper.selectList(queryWrapper);
        if (doList == null || doList.isEmpty()) {
            log.warn("[同步Maas数据集]无数据");
            return;
        }
        // 转换数据
        List<DatasetMaasSaveReqVO> saveReqList = convertData(doList);
        // 调用 新数据集Api 保存，保存后得到主键ID、数据集路径等，根据sourceId找到对应关系
        List<DatasetMaasSaveReqVO> savedList = datasetApi.createDataset(saveReqList);
        // 处理原路径（Maas）、目标路径（新数据集）
        Map<Long, DatasetRecordInfoDO> doMap = doList.stream().collect(Collectors.toMap(DatasetRecordInfoDO::getId, t -> t, (v1, v2) -> v1));
        List<SourceTargetBO> boList = new ArrayList<>();
        for (DatasetMaasSaveReqVO vo : savedList) {
            DatasetRecordInfoDO infoDO = doMap.get(vo.getSourceId());
            boList.add(new SourceTargetBO(infoDO, vo));
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
