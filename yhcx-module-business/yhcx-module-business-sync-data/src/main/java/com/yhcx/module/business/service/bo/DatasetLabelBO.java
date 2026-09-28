package com.yhcx.module.business.service.bo;

import lombok.Data;
import lombok.Getter;
import lombok.Setter;

import java.util.List;

@Data
public class DatasetLabelBO {

    private LabelInfoBO labelInfo;
    private SummaryInfoBO summaryInfo;

    @Getter
    @Setter
    public static class LabelInfoBO {
        /*
        {
            "labelType": "2_VIDEO_SEGMENT",
            "labelFormat": "VIDEO_DEFAULT",
            "labelTypeName": "视频切割",
            "labelFormatName": "MP4/AVI/TS/MOV"
        }
        */
        private String labelType;
        private String labelFormat;
        private String labelTypeName;
        private String labelFormatName;
    }


    @Getter
    @Setter
    public static class SummaryInfoBO {
        /*
        {
            "sampleCount": 5,
            "categoryList": null,
            "categoryCount": null,
            "categoryCounts": null,
            "classesStorageUrl": null
        }
        */
        private Integer sampleCount;
        private List<Object> categoryList;
        private Integer categoryCount;
        private Object categoryCounts;
        private String classesStorageUrl;
    }
}
