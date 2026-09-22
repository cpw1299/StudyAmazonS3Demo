package com.yhcx.framework.file.core.dto;


import io.swagger.v3.oas.annotations.media.Schema;
import java.util.ArrayList;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

/**
 * 文件目录 响应对象
 *
 * @author liuhm
 * @version 1.0.0
 * @create 2025/4/7 11:05
 **/
@Schema(description = "文件夹 响应对象")
@Data
@NoArgsConstructor
public class FolderRespDTO {

    public FolderRespDTO(String name, String path) {
        this.name = name;
        this.path = path;
        this.children = new ArrayList<>();
    }

    /**
     * 文件夹名称
     */
    private String name;

    /**
     * 相对/dav/dataset/ 的路径
     */
    private String path;

    private List<FolderRespDTO> children;

    // 查找子节点
    public FolderRespDTO findChild(String name) {
        for (FolderRespDTO child : children) {
            if (child.getName().equals(name)) {
                return child;
            }
        }
        return null;
    }

}
