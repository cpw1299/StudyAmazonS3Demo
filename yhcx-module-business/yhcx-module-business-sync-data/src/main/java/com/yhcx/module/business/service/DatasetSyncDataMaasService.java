package com.yhcx.module.business.service;

import com.yhcx.module.business.dal.dataobject.DatasetRecordInfoDO;
import com.yhcx.module.business.vo.DatasetSaveReqVo;

import java.util.List;

public interface DatasetSyncDataMaasService {

    /**
     * 从 dataset_record_info表 复制到 da_dataset表
     */
     List<DatasetSaveReqVo> convertData(List<DatasetRecordInfoDO> doList);

}
