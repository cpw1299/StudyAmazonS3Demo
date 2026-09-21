package com.yhcx.module.business.dal.dataobject;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.*;

import java.math.BigDecimal;
import java.time.LocalDateTime;

/**
 * 数据集配置管理记录 DO。
 *
 * @author
 */
@TableName("dataset_record_info")
@Data
@EqualsAndHashCode
@ToString(callSuper = true)
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DatasetRecordInfoDO {

    /** 主键：数据集唯一 ID */
    @TableId(type = IdType.AUTO)
    private Integer id;

    /** 租户 ID */
    private Long tenantId;

    /** 数据集类型（公有、私有） */
    private Integer datasetProperty;

    /** 数据集/数据源名称 */
    private String datasetName;

    /** 数据集/数据源 Ceph 存储路径 */
    private String datasetStoragePath;

    /** 数据集类型 */
    private String datasetType;

    /** 数据来源 */
    private String datasetSource;

    /** 标注状态：0 未标注，1 已标注 */
    private Integer annotationState;

    /** 数据集标签列表，JSON 字符串 */
    private String datasetLabel;

    /** 数据源大小（BYTE），视频场景下含义由业务定义 */
    private Long datasetSize;

    /** 数据集数据量 */
    private Integer datasetCount;

    /** 数据集描述 */
    private String datasetDesc;

    /** 创建时间 */
    private LocalDateTime gmtCreate;

    /** 修改时间 */
    private LocalDateTime gmtModified;

    /** 创建人 */
    private String createBy;

    /** 修改人 */
    private String modifiedBy;

    /** 是否删除：1 删除，0 未删除 */
    private Integer deleted;

    /** 扩展字段，JSON 字符串 */
    private String extendInfo;

    /** 用户 ID */
    private Long userId;

    /** 数据场景 */
    private String datasetScene;

    /** 是否上传：0 未上传，1 已上传 */
    private Integer uploadState;

    /** 抽帧频率，单位：秒/帧 */
    private BigDecimal frameExtraction;

    /** 数据集状态 */
    private String status;

    /** 音频切割规则 */
    private String audioSplit;

    /** 慧衍侧数据集 ID */
    private Long parentId;
}
