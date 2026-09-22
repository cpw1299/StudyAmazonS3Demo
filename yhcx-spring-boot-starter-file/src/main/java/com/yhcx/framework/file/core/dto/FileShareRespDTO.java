package com.yhcx.framework.file.core.dto;


import io.swagger.v3.oas.annotations.media.Schema;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import lombok.Data;
import lombok.experimental.Accessors;

/**
 * 文件共享 响应对象
 *
 * @author liuhm
 * @version 1.0.0
 * @create 2025/4/7 11:05
 **/
@Schema(description = "文件共享 响应对象")
@Data
@Accessors(chain = true)
public class FileShareRespDTO {

    /**
     * 文件原始名称
     */
    private String name;

    /**
     * 共享地址
     */
    private String sharePath;

}
