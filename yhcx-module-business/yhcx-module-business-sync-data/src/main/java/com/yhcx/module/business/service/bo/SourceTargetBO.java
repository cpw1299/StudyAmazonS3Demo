package com.yhcx.module.business.service.bo;

import com.yhcx.module.business.dal.dataobject.DatasetRecordInfoDO;
import com.yhcx.module.business.vo.DatasetSaveReqVo;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class SourceTargetBO {

    private DatasetRecordInfoDO source;
    private DatasetSaveReqVo target;
}
