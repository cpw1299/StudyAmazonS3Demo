package com.yhcx.module.business.dal.dataobject;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.*;

import java.time.LocalDateTime;

/**
 * 数据导入明细 DO。
 *
 * @author
 */
@TableName("dataset_record_detail_info")
@Data
@EqualsAndHashCode
@ToString(callSuper = true)
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DatasetRecordDetailInfoDO {

    /** 主键：导入元数据唯一标识 */
    @TableId(type = IdType.AUTO)
    private Long id;

    /** 租户 ID */
    private Long tenantId;

    /** 数据源/数据集 ID */
    private Integer datasetId;

    /** 批次 ID */
    private String batchId;

    /** 上传文件名称 */
    private String fileName;

    /** 文件扩展类型 */
    private String fileType;

    /** 文件父级目录 */
    private String fileParentPath;

    /** 是否需要解压：0 否，1 是 */
    private Integer needCompress;

    /** 上传文件存储地址 */
    private String storageUrl;

    /** 异常信息 */
    private String errorMsg;

    /** 数据大小（Byte） */
    private Long fileSize;

    /** 文件上传状态：uploading / success / error */
    private String fileStatus;

    /** 文件分片上传标识 ID */
    private String uploadId;

    /** 上传进度 */
    private Integer uploadProgress;

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

    /** 用户 ID */
    private Long userId;

    /** 扩展字段，JSON 字符串 */
    private String extendInfo;
}
