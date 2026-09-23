package com.yhcx.module.business.dal.mysql;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.yhcx.module.business.dal.dataobject.DatasetFileSyncRecordDO;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Update;

@Mapper
public interface DatasetFileSyncRecordMapper extends BaseMapper<DatasetFileSyncRecordDO> {

    @Update("UPDATE dataset_file_sync_record " +
            "SET total_file_count = #{totalFileCount}, " +
            "    status = #{status}, " +
            "    error_message = #{errorMessage}, " +
            "    finish_time = #{finishTime}, " +
            "    update_time = NOW() " +
            "WHERE id = #{id}")
    int updateProgress(DatasetFileSyncRecordDO record);
}
