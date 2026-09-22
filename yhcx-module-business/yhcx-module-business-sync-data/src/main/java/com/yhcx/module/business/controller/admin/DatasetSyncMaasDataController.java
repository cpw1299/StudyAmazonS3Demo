package com.yhcx.module.business.controller.admin;

import com.yhcx.framework.common.pojo.CommonResult;
import com.yhcx.module.business.service.DatasetSyncMaasService;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import java.util.List;

@RestController
@RequestMapping("/ds/maas")
public class DatasetSyncMaasDataController {

    @Resource
    private DatasetSyncMaasService datasetSyncMaasService;

    /*
        DatasetSyncMaasDataController
            ↓
        DatasetSyncMaasServiceImpl
            ↓
        查询 dataset_record_info
            ↓
        创建新数据集
            ↓
        得到 source → target 对应关系
            ↓
        DatasetSyncMaasEvnet
            ↓
        DatasetFileSyncMaasListener
            ↓
        DatasetFileSyncMaasService
            ↓
        复制 S3/MinIO 文件
     */

    /**
     *
     * @param datasetRecordIds
     * @return
     */
    @PostMapping(value = "/sync/data")
    public CommonResult<Object> syncData(List<Long> datasetRecordIds) {
        // 不为空时，同步指定数据
        datasetSyncMaasService.copyDatasetRecordToDataset(datasetRecordIds);
        return CommonResult.success(null);
    }
}
