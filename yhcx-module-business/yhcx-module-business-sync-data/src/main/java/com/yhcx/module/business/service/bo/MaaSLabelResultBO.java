package com.yhcx.module.business.service.bo;

import lombok.Data;

import java.io.Serializable;
import java.util.List;

@Data
public class MaaSLabelResultBO implements Serializable {

    private static final long serialVersionUID = 1L;


    private MaaSLabelInfo labelInfo;

    private MaaSSummaryInfo summaryInfo;

    @Data
    public static class MaaSLabelInfo implements Serializable{

        private static final long serialVersionUID = 1L;
        /**
         * 标注场景  (必填)
         */
        private String labelType;
        private String labelTypeName;

        /**
         * 标注格式  (必填)
         */
        private String labelFormat;
        private String labelFormatName;
    }

    @Data
    public static class MaaSSummaryInfo implements Serializable{
        private static final long serialVersionUID = 1L;
        /**
         * 样本数量
         */
        private Integer sampleCount;
        /**
         * 类别ID列表
         */
        private List<String> categoryList;
        /**
         * 类别总数
         */
        private Integer categoryCount;
        /**
         * 类别文件对象存储路径
         */
        private String classesStorageUrl;
    }
}
