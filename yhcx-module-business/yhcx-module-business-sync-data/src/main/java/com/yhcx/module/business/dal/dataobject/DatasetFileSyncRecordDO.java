package com.yhcx.module.business.dal.dataobject;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.*;

import java.time.LocalDateTime;

/**
 * 数据集文件同步任务记录。
 */
@TableName("dataset_file_sync_record")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DatasetFileSyncRecordDO {

    @TableId(type = IdType.AUTO)
    private Long id;

    private Long sourceDatasetId;
    private Long targetDatasetId;

    private Long totalFileCount;
    private Long successFileCount;
    private Long failedFileCount;

    private Long totalFileSize;
    private Long successFileSize;

    private String status;
    private String errorMessage;

    private LocalDateTime startTime;
    private LocalDateTime finishTime;

    private LocalDateTime createTime;
    private LocalDateTime updateTime;
}
