package com.yhcx.module.business.service;

import java.util.List;

public interface DatasetSyncMaasService {

    /**
     * 从 dataset_record_info表 复制到 da_dataset表
     */
    void copyDatasetRecordToDataset(List<Long> datasetRecordIds);

}
