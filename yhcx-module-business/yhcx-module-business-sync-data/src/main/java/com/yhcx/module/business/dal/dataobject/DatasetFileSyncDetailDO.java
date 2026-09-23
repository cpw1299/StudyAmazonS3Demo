package com.yhcx.module.business.dal.dataobject;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.*;

import java.time.LocalDateTime;

/**
 * 数据集文件同步明细。
 */
@TableName("dataset_file_sync_detail")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DatasetFileSyncDetailDO {

    @TableId(type = IdType.AUTO)
    private Long id;

    private Long syncRecordId;
    private Long sourceDatasetId;
    private Long targetDatasetId;

    private String sourceFileKey;
    private String targetFileKey;

    private Long fileSize;
    private String status;
    private String errorMessage;
    private Integer retryCount;

    private LocalDateTime createTime;
    private LocalDateTime updateTime;
}
