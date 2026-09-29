package com.yhcx.module.business.service;

import cn.hutool.core.date.DateUtil;
import cn.hutool.core.date.LocalDateTimeUtil;
import cn.hutool.core.util.IdUtil;
import cn.hutool.core.util.ObjectUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.alibaba.fastjson2.JSONObject;
import com.yhcx.framework.common.exception.enums.GlobalErrorCodeConstants;
import com.yhcx.module.business.api.DatasetApi;
import com.yhcx.module.business.api.TagApi;
import com.yhcx.module.business.constant.BusinessConstants;
import com.yhcx.module.business.dal.dataobject.DatasetRecordInfoDO;
import com.yhcx.module.business.dal.mysql.DatasetRecordInfoMapper;
import com.yhcx.module.business.enums.*;
import com.yhcx.module.business.service.bo.MaaSLabelResultBO;
import com.yhcx.module.business.vo.DatasetSaveReqVo;
import com.yhcx.module.infra.DatasetPathUtil;
import com.yhcx.module.infra.enums.DatasetPathEnum;
import com.yhcx.module.system.api.dept.DeptApi;
import com.yhcx.module.system.api.user.AdminUserApi;
import com.yhcx.module.system.api.user.dto.AdminMaaSUserRespDTO;
import com.yhcx.module.system.constats.SystemConstants;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import static com.yhcx.framework.common.exception.util.ServiceExceptionUtil.exception;

@Slf4j
@Service
@RequiredArgsConstructor
public class DatasetSyncDataMaasServiceImpl implements DatasetSyncDataMaasService {

    private final DatasetRecordInfoMapper datasetRecordInfoMapper;

    private final DatasetApi datasetApi;

    private final TagApi tagApi;

    private final ApplicationEventPublisher applicationEventPublisher;

    private final DatasetFileSyncRecordService recordService;

    @Resource
    private DeptApi deptApi;

    @Resource
    private AdminUserApi adminUserApi;


    /**
     * 日志统一前缀（便于日志过滤与检索）
     */
    private static final String LOG_PREFIX = "[maas数据迁移-数据集服务]";

    @Override
    public List<DatasetSaveReqVo> convertData(List<DatasetRecordInfoDO> doList) {
        List<DatasetSaveReqVo> resultList = new ArrayList<>();
        List<DatasetRecordInfoDO> verificationFailedList = new ArrayList<>();
        List<Long> verificationFailedIds = new ArrayList<>();
        // 查标签表ds_tag
        Map<Integer, Map<String, Long>> datTypeTagMap = tagApi.getDataTypeNameIdMap();
        for (DatasetRecordInfoDO infoDO : doList) {
            Long datasetRecordId = infoDO.getId();
            try {
                DatasetSaveReqVo datasetSaveReqVo = convertSingleData(infoDO, datTypeTagMap);
                resultList.add(datasetSaveReqVo);
            } catch (Exception e) {
                log.error("【数据集同步转换】单条数据集转换失败，recordId:{}", datasetRecordId, e);
//                throw exception(GlobalErrorCodeConstants.BAD_REQUEST, "数据集转换失败,recordId:" + datasetRecordId, e);
                verificationFailedList.add(infoDO);
                verificationFailedIds.add(infoDO.getId());
            }
        }
        if (!verificationFailedList.isEmpty()) {
            log.error("【数据集同步转换】验证失败的数据：{}", verificationFailedIds);
        }
        return resultList;
    }

    private DatasetSaveReqVo convertSingleData(
            DatasetRecordInfoDO recordInfoDO,
            Map<Integer, Map<String, Long>> datTypeTagMap) {
        Long maasCompanyId = recordInfoDO.getTenantId();
        // MaaS租户默认名称（类成员变量）
        String maasDefaultName = BusinessConstants.maasTemaDefaultName;
        Long maasId = recordInfoDO.getId();
        String maasCreateBy = recordInfoDO.getCreateBy();

        // 如果外部扩展数据集已经存在，直接返回已有VO，不再重新初始化
        DatasetSaveReqVo existingDataset = findExistingDataset(maasId);
        if (existingDataset != null) {
            return existingDataset;
        }
        // RPC远程调用MaaS用户租户信息
        AdminMaaSUserRespDTO companyTemaIdUserIdSourceByMaaS = adminUserApi.getCompanyTemaIdUserIdSourceByMaaS(maasCompanyId, maasDefaultName,  maasCreateBy);
        // 字段以及标注元数据校验，同时解析已标注数据集的标注信息
        LabelValidationResult labelValidationResult =
                validateData(companyTemaIdUserIdSourceByMaaS, recordInfoDO, datTypeTagMap);
        return buildDatasetSaveReqVo(companyTemaIdUserIdSourceByMaaS, recordInfoDO, labelValidationResult);
    }

    private DatasetSaveReqVo findExistingDataset(Long maasId) {
        return datasetApi.getDatasetSaveReqVoByExtendDatasetId(
                BusinessConstants.DATASET_SYN_TENANTID_ID,
                SystemConstants.CLIENT.MAAS_CLIENT_ID,
                String.valueOf(maasId));
    }

    private LabelValidationResult validateData(
            AdminMaaSUserRespDTO companyTemaIdUserIdSourceByMaaS,
            DatasetRecordInfoDO recordInfoDO,
            Map<Integer, Map<String, Long>> datTypeTagMap) {
        log.warn("[MaaS查询返回对象]：{}", JSONObject.toJSONString(companyTemaIdUserIdSourceByMaaS));
        // 1、校验MaaS侧返回对象基础字段
        if (companyTemaIdUserIdSourceByMaaS == null) {
            log.error("{} MaaS查询返回对象为空", LOG_PREFIX);
            throw exception(GlobalErrorCodeConstants.BAD_REQUEST, "MaaS数据集信息查询为空");
        }
        if (companyTemaIdUserIdSourceByMaaS.getCompanyId() == null
                && companyTemaIdUserIdSourceByMaaS.getUserCompanyId() == null) {
            log.error("{} MaaS返回companyId为空", LOG_PREFIX);
            throw exception(GlobalErrorCodeConstants.BAD_REQUEST, "MaaS数据集组织ID缺失");
        }
        if (companyTemaIdUserIdSourceByMaaS.getMaasTemaDefaultId() == null) {
            log.error("{} MaaS返回maasTemaDefaultId为空", LOG_PREFIX);
            throw exception(GlobalErrorCodeConstants.BAD_REQUEST, "MaaS租户标识缺失");
        }
        if (companyTemaIdUserIdSourceByMaaS.getUserId() == null) {
            log.error("{} MaaS返回userId为空", LOG_PREFIX);
            throw exception(GlobalErrorCodeConstants.BAD_REQUEST, "MaaS操作用户ID缺失");
        }

        // 2、校验数据集记录DO基础字段
        if (recordInfoDO.getDatasetName() == null) {
            log.error("{} 数据集名称datasetName为空", LOG_PREFIX);
            throw exception(GlobalErrorCodeConstants.BAD_REQUEST, "数据集名称不能为空");
        }
        if (recordInfoDO.getDatasetType() == null) {
            log.error("{} 数据集类型datasetType为空", LOG_PREFIX);
            throw exception(GlobalErrorCodeConstants.BAD_REQUEST, "数据集类型不能为空");
        }
        if (recordInfoDO.getAnnotationState() == null) {
            log.error("{} 数据集标注状态annotationState为空", LOG_PREFIX);
            throw exception(GlobalErrorCodeConstants.BAD_REQUEST, "数据集标注状态不能为空");
        }

        // 数据类型及标注元数据校验
        LabelValidationResult labelValidationResult =
                validateAndParseLabelInfo(recordInfoDO, datTypeTagMap);

        // 4、数据集解析成功状态：校验存储路径
        final String PARSE_SUCCESS = "PARSE_SUCCESS";
        if (PARSE_SUCCESS.equals(recordInfoDO.getStatus())) {
            if (recordInfoDO.getDatasetStoragePath() == null) {
                log.error("{} 数据集状态为PARSE_SUCCESS，但是datasetStoragePath文件存储路径为空", LOG_PREFIX);
                throw exception(GlobalErrorCodeConstants.BAD_REQUEST, "数据集解析成功，但缺少数据集存储路径");
            }
        }

        return labelValidationResult;
    }

    private LabelValidationResult validateAndParseLabelInfo(
            DatasetRecordInfoDO recordInfoDO,
            Map<Integer, Map<String, Long>> datTypeTagMap) {

        //  数据类型转换
        Integer codeByMaas = DataTypeEnum.getCodeByMaas(recordInfoDO.getDatasetType());
        if (codeByMaas == null) {
            log.error("{} 数据类型不匹配", LOG_PREFIX);
            throw exception(GlobalErrorCodeConstants.BAD_REQUEST, "数据类型不匹配");
        }

        // 3、已经标注的数据集：校验datasetLabel以及解析标注元信息
        Integer annotationState = recordInfoDO.getAnnotationState();
        if (YesOrNoEnum.YES_1.getCode().equals(annotationState)) {
            // 已标注，标注元信息json字段不可为空
            if (recordInfoDO.getDatasetLabel() == null) {
                log.error("{} 已标注数据集，datasetLabel标注元信息为空", LOG_PREFIX);
                throw exception(GlobalErrorCodeConstants.BAD_REQUEST, "已标注数据集缺少标注元数据");
            }
            String datasetLabel = recordInfoDO.getDatasetLabel();
            MaaSLabelResultBO maaSLabelResultBO;
            try {
                maaSLabelResultBO = JSONUtil.toBean(datasetLabel, MaaSLabelResultBO.class);
            } catch (Exception e) {
                log.error("{} datasetLabel JSON反序列化失败，rawValue:{}", LOG_PREFIX, datasetLabel, e);
                throw exception(GlobalErrorCodeConstants.BAD_REQUEST, "数据集标注元数据JSON解析异常");
            }
            if (maaSLabelResultBO == null || maaSLabelResultBO.getLabelInfo() == null) {
                log.error("{} MaaSLabelResultBO#labelInfo为空，标注元数据缺少labelInfo节点", LOG_PREFIX);
                throw exception(GlobalErrorCodeConstants.BAD_REQUEST, "数据集标注基础信息缺失");
            }
            MaaSLabelResultBO.MaaSLabelInfo labelInfo = maaSLabelResultBO.getLabelInfo();
            // -------- 后续 TODO 标注类型：
            // 2、读取summaryInfo.classesStorageUrl，下载classes.txt类别文件，构建类别ID‑名称映射
            String labelType = labelInfo.getLabelType();

            if (labelType == null) {
                log.error("{} MaaSLabelResultBO#labelInfo->labelType为空，标注元数据缺少labelInfo->labelType", LOG_PREFIX);
                throw exception(GlobalErrorCodeConstants.BAD_REQUEST, "数据集标注基础信息缺失");
            }

            Map<String, Long> labelTypeMap = datTypeTagMap.get(codeByMaas);
            Long labelTypeId = labelTypeMap == null ? null : labelTypeMap.get(labelType);
            if (labelTypeId == null) {
                log.error("{} MaaSLabelResultBO#labelInfo->labelType->idByDataTypeMaasLabelType 不匹配", LOG_PREFIX);
                throw exception(GlobalErrorCodeConstants.BAD_REQUEST,
                        " MaaSLabelResultBO#labelInfo->labelType->idByDataTypeMaasLabelType=null 不匹配");
            }

            return new LabelValidationResult(labelInfo, labelTypeId);
        }

        return null;

    }


    private DatasetSaveReqVo buildDatasetSaveReqVo(
            AdminMaaSUserRespDTO companyTemaIdUserIdSourceByMaaS,
            DatasetRecordInfoDO recordInfoDO,
            LabelValidationResult labelValidationResult) {

        DatasetSaveReqVo reqVO = new DatasetSaveReqVo();

        //租户
        reqVO.setTenantId(BusinessConstants.DATASET_SYN_TENANTID_ID);
        reqVO.setClientId(SystemConstants.CLIENT.MAAS_CLIENT_ID);

        //外部数据集主键
        Long id = recordInfoDO.getId();
        reqVO.setExtendDatasetId(String.valueOf(id));
        //企业主键
//        reqVO.setCompanyId(String.valueOf(companyTemaIdUserIdSourceByMaaS.getCompanyId()));
        reqVO.setCompanyId(String.valueOf(companyTemaIdUserIdSourceByMaaS.getUserCompanyId()));
        //团队主键
        reqVO.setTeamId(String.valueOf(companyTemaIdUserIdSourceByMaaS.getMaasTemaDefaultId()));
        reqVO.setParentId(0L);
        reqVO.setVersionCode(BusinessConstants.DATASET_DEFAULT_VERSION);

        reqVO.setProjectId(null);
        reqVO.setEmqFileTemplateId(null);
        reqVO.setEmqTableTemplateId(null);

        //是否结构模态
        reqVO.setDataUnStructuredFlag(YesOrNoEnum.NO_0.getCode());
        reqVO.setDataStructuredModals(null);

        reqVO.setSceneId(null);
        reqVO.setPartId(null);
        reqVO.setPartIds(null);

        reqVO.setLicense(DatasetLicenseEnum.APACHE_2_0.getCode());
        reqVO.setOwner(null);
        reqVO.setIndustryType(DatasetIndustryTypeEnum.INDUSTRY_PROFESSIONAL.getCode());
        //基础常识数据 默认(待设置)
        reqVO.setResourceId(BusinessConstants.id_default_999);
        reqVO.setResourceLifecycleId(null);

        reqVO.setPublicType(null);
        //如果是更新不需要重新生产
        reqVO.setUniqueCode(DateUtil.format(new Date(), "yyyyMMddHHmmssSSS"));
        reqVO.setName(recordInfoDO.getDatasetName());

        //--------------------------------------------------
        reqVO.setLabelFlag(recordInfoDO.getAnnotationState());

        //TODO 数据类型转换
        reqVO.setDataType(DataTypeEnum.getCodeByMaas(recordInfoDO.getDatasetType()));
        reqVO.setDataStructuredModals(String.valueOf(reqVO.getDataType()));
        //TODO 需要做数据转换

        //TODO  数据集类型 DatasetTypeEnum 未标注数据是 原始数据集  如果为已标注数据 则为标注数据集
        if (ObjectUtil.equals(YesOrNoEnum.YES_1.getCode(), reqVO.getLabelFlag())) {
            reqVO.setDatasetType(DatasetTypeEnum.LABEL_DATA.getCode());

            MaaSLabelResultBO.MaaSLabelInfo maaSLabelInfo = labelValidationResult.getLabelInfo();
            if (maaSLabelInfo != null) {
                reqVO.setLabelType(labelValidationResult.getLabelTypeId());
                //BusinessConstants.LABELFORMAT_DEFAULT); //只要是慧衍的数据集，都返回COCO
                reqVO.setLabelFormat(maaSLabelInfo.getLabelFormat());
            }
        } else {
            reqVO.setDatasetType(DatasetTypeEnum.RAW_DATA.getCode());
        }

        reqVO.setProcessType(null);
        reqVO.setExtentSceneType(null);
        reqVO.setDatasetStatus(DatasetStatusEnum.UN_COMPLETED.getCode()); // 默认状态：未完成
        // 数据集日期，默认为当前时间
        reqVO.setDatasetDate(
                ObjectUtil.defaultIfNull(recordInfoDO.getGmtCreate(), LocalDateTimeUtil.now()));
        reqVO.setDatasetReleaseType(DatasetReleaseTypeEnum.UN_SHARED.getCode()); // 发布类型：中车

        //share_approval_status
        //share_instance_id
        reqVO.setShareStatus(DatasetShareStatusEnum.UN_SHARED.getCode()); // 默认共享状态：未共享
        //syn_status
        reqVO.setMarkStatus(DatasetMarkStatusEnum.NO.getCode()); // 默认

        // 生成仓库ID与仓库路径 TODO 如果是修改是不需要变动
        String repositoryId = StrUtil.format("{}_{}", reqVO.getClientId(), IdUtil.fastSimpleUUID());
        reqVO.setRepositoryId(repositoryId);

        // 组装草稿分区文件存储路径  TODO 如果是修改是不需要变动
        String repositoryPath = DatasetPathUtil.concatRelativePath(DatasetPathEnum.DATASET_UNPUBLISH,
                reqVO.getCompanyId(),
                reqVO.getTeamId(),
                BusinessConstants.maasTemaDefaultName,
                reqVO.getName());
        reqVO.setRepositoryPath(repositoryPath);

        /**归档地址，访问地址(数据集【压缩文件】zip相对路径,压缩文件格式固定为:zip)*/
        reqVO.setArchiveUrl(null);
        /**归档地址，存储路径（数据集【文件夹】相对路径）*/
        reqVO.setArchivePath(null);
        reqVO.setArchiveStatus(DatasetArchiveStatusEnum.UNARCHIVED.getCode());
        reqVO.setFileCount(0L);
        reqVO.setFileSize(0L);
        reqVO.setTableCount(null);
        reqVO.setTableSize(null);

        // 创建人信息
        reqVO.setCreator(String.valueOf(companyTemaIdUserIdSourceByMaaS.getUserId()));
        reqVO.setCreateTime(reqVO.getDatasetDate());

        //元数据
        String datasetDesc = recordInfoDO.getDatasetDesc();
        reqVO.setInstructions(null);
        if(datasetDesc!=null && datasetDesc !="" && datasetDesc.length() > 1){
            reqVO.setInstructions(datasetDesc);
        }
        reqVO.setDatasetMeta(null);
        reqVO.setLabelsMeta(null);
        reqVO.setFieldsDesc(null);

        return reqVO;
    }

    /**
     * 单条数据校验阶段产出的结果。
     * 校验与解析只做一次，后续数据对象组装直接复用，避免重复查询和解析。
     */
    private static final class LabelValidationResult {

        private final MaaSLabelResultBO.MaaSLabelInfo labelInfo;

        private final Long labelTypeId;

        private LabelValidationResult(MaaSLabelResultBO.MaaSLabelInfo labelInfo, Long labelTypeId) {
            this.labelInfo = labelInfo;
            this.labelTypeId = labelTypeId;
        }

        private MaaSLabelResultBO.MaaSLabelInfo getLabelInfo() {
            return labelInfo;
        }

        private Long getLabelTypeId() {
            return labelTypeId;
        }
    }


}
