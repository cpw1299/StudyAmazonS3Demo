package com.yhcx.module.business.service;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.yhcx.module.business.dal.dataobject.DatasetFileSyncDetailDO;
import com.yhcx.module.business.dal.dataobject.DatasetFileSyncRecordDO;
import com.yhcx.module.business.dal.mysql.DatasetFileSyncDetailMapper;
import com.yhcx.module.business.dal.mysql.DatasetFileSyncRecordMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;

/**
 * 数据集文件同步状态持久化服务。
 */
@Service
@RequiredArgsConstructor
public class DatasetFileSyncRecordService {

    public static final String STATUS_PROCESSING = "PROCESSING";
    public static final String STATUS_SUCCESS = "SUCCESS";
    public static final String STATUS_FAILED = "FAILED";

    public static final String FILE_STATUS_PENDING = "PENDING";
    public static final String FILE_STATUS_SUCCESS = "SUCCESS";
    public static final String FILE_STATUS_FAILED = "FAILED";

    private final DatasetFileSyncRecordMapper recordMapper;
    private final DatasetFileSyncDetailMapper detailMapper;

    public DatasetFileSyncRecordDO getOrCreate(Long sourceDatasetId, Long targetDatasetId) {
        LambdaQueryWrapper<DatasetFileSyncRecordDO> wrapper = new LambdaQueryWrapper<>();
        wrapper.eq(DatasetFileSyncRecordDO::getSourceDatasetId, sourceDatasetId)
                .eq(DatasetFileSyncRecordDO::getTargetDatasetId, targetDatasetId)
                .last("LIMIT 1");
        DatasetFileSyncRecordDO record = recordMapper.selectOne(wrapper);
        if (record != null) {
            return record;
        }

        LocalDateTime now = LocalDateTime.now();
        record = DatasetFileSyncRecordDO.builder()
                .sourceDatasetId(sourceDatasetId)
                .targetDatasetId(targetDatasetId)
                .totalFileCount(0L)
                .successFileCount(0L)
                .failedFileCount(0L)
                .totalFileSize(0L)
                .successFileSize(0L)
                .status(STATUS_PROCESSING)
                .startTime(now)
                .createTime(now)
                .updateTime(now)
                .build();
        recordMapper.insert(record);
        return record;
    }

    public DatasetFileSyncDetailDO findDetail(Long syncRecordId, String sourceFileKey) {
        LambdaQueryWrapper<DatasetFileSyncDetailDO> wrapper = new LambdaQueryWrapper<>();
        wrapper.eq(DatasetFileSyncDetailDO::getSyncRecordId, syncRecordId)
                .eq(DatasetFileSyncDetailDO::getSourceFileKey, sourceFileKey)
                .last("LIMIT 1");
        return detailMapper.selectOne(wrapper);
    }

    public DatasetFileSyncDetailDO getOrCreateDetail(DatasetFileSyncRecordDO record,
                                                      String sourceFileKey,
                                                      String targetFileKey,
                                                      long fileSize) {
        DatasetFileSyncDetailDO detail = findDetail(record.getId(), sourceFileKey);
        if (detail == null) {
            LocalDateTime now = LocalDateTime.now();
            detail = DatasetFileSyncDetailDO.builder()
                    .syncRecordId(record.getId())
                    .sourceDatasetId(record.getSourceDatasetId())
                    .targetDatasetId(record.getTargetDatasetId())
                    .sourceFileKey(sourceFileKey)
                    .targetFileKey(targetFileKey)
                    .fileSize(fileSize)
                    .status(FILE_STATUS_PENDING)
                    .retryCount(0)
                    .createTime(now)
                    .updateTime(now)
                    .build();
            detailMapper.insert(detail);
            return detail;
        }

        if (!targetFileKey.equals(detail.getTargetFileKey()) || detail.getFileSize() == null
                || detail.getFileSize() != fileSize) {
            detail.setTargetFileKey(targetFileKey);
            detail.setFileSize(fileSize);
            if (!FILE_STATUS_SUCCESS.equals(detail.getStatus())) {
                detail.setStatus(FILE_STATUS_PENDING);
            }
            detail.setErrorMessage(null);
            detailMapper.updateById(detail);
        }
        return detail;
    }

    public void markProcessing(DatasetFileSyncRecordDO record) {
        record.setStatus(STATUS_PROCESSING);
        record.setStartTime(LocalDateTime.now());
        record.setFinishTime(null);
        record.setErrorMessage(null);
        recordMapper.updateById(record);
    }

    public void updateTotalFileCount(DatasetFileSyncRecordDO record, long totalFileCount) {
        record.setTotalFileCount(totalFileCount);
        record.setUpdateTime(LocalDateTime.now());
        recordMapper.updateById(record);
    }

    public void markSuccess(DatasetFileSyncRecordDO record) {
        record.setStatus(STATUS_SUCCESS);
        record.setFailedFileCount(0L);
        record.setErrorMessage(null);
        record.setFinishTime(LocalDateTime.now());
        recordMapper.updateById(record);
    }

    public void markFilePending(DatasetFileSyncDetailDO detail) {
        detail.setStatus(FILE_STATUS_PENDING);
        detail.setErrorMessage(null);
        detail.setRetryCount((detail.getRetryCount() == null ? 0 : detail.getRetryCount()) + 1);
        detailMapper.updateById(detail);
    }

    public void markFileSuccess(DatasetFileSyncDetailDO detail) {
        detail.setStatus(FILE_STATUS_SUCCESS);
        detail.setErrorMessage(null);
        detailMapper.updateById(detail);
    }

    public void markFileFailed(DatasetFileSyncDetailDO detail, Exception e) {
        detail.setStatus(FILE_STATUS_FAILED);
        detail.setErrorMessage(buildErrorMessage(e));
        detailMapper.updateById(detail);
    }

    public void recordFileSuccess(DatasetFileSyncRecordDO record,
                                  DatasetFileSyncDetailDO detail,
                                  String previousStatus) {
        markFileSuccess(detail);
        long successCount = record.getSuccessFileCount() == null ? 0L : record.getSuccessFileCount();
        long failedCount = record.getFailedFileCount() == null ? 0L : record.getFailedFileCount();
        if (!FILE_STATUS_SUCCESS.equals(previousStatus)) {
            successCount++;
        }
        if (FILE_STATUS_FAILED.equals(previousStatus) && failedCount > 0) {
            failedCount--;
        }
        record.setSuccessFileCount(successCount);
        record.setFailedFileCount(failedCount);
        record.setStatus(failedCount > 0 ? STATUS_FAILED
                : (successCount >= (record.getTotalFileCount() == null ? 0L : record.getTotalFileCount())
                ? STATUS_SUCCESS : STATUS_PROCESSING));
        record.setErrorMessage(failedCount > 0 ? "存在文件同步失败，请查看 dataset_file_sync_detail" : null);
        record.setFinishTime(STATUS_SUCCESS.equals(record.getStatus()) ? LocalDateTime.now() : null);
        recordMapper.updateById(record);
    }

    public void recordFileFailed(DatasetFileSyncRecordDO record,
                                 DatasetFileSyncDetailDO detail,
                                 String previousStatus,
                                 Exception e) {
        markFileFailed(detail, e);
        long failedCount = record.getFailedFileCount() == null ? 0L : record.getFailedFileCount();
        if (!FILE_STATUS_FAILED.equals(previousStatus)) {
            failedCount++;
        }
        record.setFailedFileCount(failedCount);
        record.setStatus(STATUS_FAILED);
        record.setErrorMessage("存在文件同步失败，请查看 dataset_file_sync_detail");
        record.setFinishTime(LocalDateTime.now());
        recordMapper.updateById(record);
    }

    public void finish(DatasetFileSyncRecordDO record) {
        long successCount = record.getSuccessFileCount() == null ? 0L : record.getSuccessFileCount();
        long failedCount = record.getFailedFileCount() == null ? 0L : record.getFailedFileCount();
        long totalCount = record.getTotalFileCount() == null ? 0L : record.getTotalFileCount();
        record.setStatus(failedCount > 0 ? STATUS_FAILED
                : (successCount >= totalCount ? STATUS_SUCCESS : STATUS_PROCESSING));
        record.setErrorMessage(failedCount > 0 ? "存在文件同步失败，请查看 dataset_file_sync_detail" : null);
        record.setFinishTime(STATUS_SUCCESS.equals(record.getStatus()) || STATUS_FAILED.equals(record.getStatus())
                ? LocalDateTime.now() : null);
        recordMapper.updateById(record);
    }

    private String buildErrorMessage(Exception e) {
        String message = e.getMessage();
        if (message == null || message.length() > 2000) {
            message = e.getClass().getSimpleName();
        }
        return message;
    }
}
