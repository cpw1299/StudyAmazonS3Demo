-- 数据集文件同步任务表
CREATE TABLE IF NOT EXISTS dataset_file_sync_record (
    id BIGINT NOT NULL AUTO_INCREMENT COMMENT '主键',
    source_dataset_id BIGINT NOT NULL COMMENT '旧数据集ID',
    target_dataset_id BIGINT NOT NULL COMMENT '新数据集ID',
    total_file_count BIGINT NOT NULL DEFAULT 0 COMMENT '源数据集文件总数',
    success_file_count BIGINT NOT NULL DEFAULT 0 COMMENT '成功同步文件数',
    failed_file_count BIGINT NOT NULL DEFAULT 0 COMMENT '失败文件数',
    total_file_size BIGINT NOT NULL DEFAULT 0 COMMENT '源文件总大小',
    success_file_size BIGINT NOT NULL DEFAULT 0 COMMENT '成功同步文件总大小',
    status VARCHAR(32) NOT NULL COMMENT 'PROCESSING/SUCCESS/FAILED',
    error_message TEXT COMMENT '最近一次失败信息',
    start_time DATETIME DEFAULT NULL COMMENT '开始同步时间',
    finish_time DATETIME DEFAULT NULL COMMENT '结束同步时间',
    create_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    update_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uk_source_target (source_dataset_id, target_dataset_id),
    KEY idx_status (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='数据集文件同步任务记录';

-- 数据集文件同步明细表
CREATE TABLE IF NOT EXISTS dataset_file_sync_detail (
    id BIGINT NOT NULL AUTO_INCREMENT COMMENT '主键',
    sync_record_id BIGINT NOT NULL COMMENT '同步任务ID',
    source_dataset_id BIGINT NOT NULL COMMENT '旧数据集ID',
    target_dataset_id BIGINT NOT NULL COMMENT '新数据集ID',
    source_file_key VARCHAR(1024) NOT NULL COMMENT '源文件Key',
    target_file_key VARCHAR(1024) NOT NULL COMMENT '目标文件Key',
    file_size BIGINT NOT NULL DEFAULT 0 COMMENT '文件大小',
    status VARCHAR(32) NOT NULL COMMENT 'PENDING/SUCCESS/FAILED',
    error_message TEXT COMMENT '失败原因',
    retry_count INT NOT NULL DEFAULT 0 COMMENT '重试次数',
    create_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    update_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uk_sync_file (sync_record_id, source_file_key(255)),
    KEY idx_sync_record_status (sync_record_id, status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='数据集文件同步明细';

-- 说明：
-- 1. 不修改 dataset_record_info，也不写入新数据集表。
-- 2. dataset_file_sync_record 保存数据集级进度；dataset_file_sync_detail 保存文件级结果。
-- 3. 同一 source_dataset_id + target_dataset_id 只允许存在一个任务记录。
-- 4. Redis 仅用于运行期分布式锁，不作为同步结果的唯一持久化存储。
