package com.yhcx.framework.file.core.convert;

import cn.hutool.core.io.file.FileNameUtil;
import cn.hutool.core.io.unit.DataSizeUtil;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.yhcx.framework.common.util.file.Tools;
import com.yhcx.framework.file.core.dto.FileRespDTO;
import com.yhcx.framework.file.core.dto.FolderRespDTO;
import org.apache.commons.lang3.StringUtils;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * S3Convert
 *
 * @author liuhm
 * @version 1.0.0
 * @create 2025/11/13 11:14
 **/
public interface S3Convert {


    static void listFile(String rootPath, String path, List<S3ObjectSummary> objectSummaries,
                         List<FileRespDTO> fileList) {
        listFile(rootPath, path, null, objectSummaries, fileList);
    }

    /**
     * 转换 文件列表
     */
    static void listFile(String rootPath, String path, String storagePath, List<S3ObjectSummary> objectSummaries,
                         List<FileRespDTO> fileList) {
        String folderStoragePath = storagePath + "/";
        for (S3ObjectSummary s3ObjectSummary : objectSummaries) {
            if (storagePath != null && (s3ObjectSummary.getKey().equals(storagePath) || s3ObjectSummary.getKey().equals(folderStoragePath))) {
                continue;
            }
            FileRespDTO respDto = new FileRespDTO();
            respDto.setName(FileNameUtil.getName(s3ObjectSummary.getKey()));
            respDto.setDisplayName(FileNameUtil.getName(s3ObjectSummary.getKey()));
            respDto.setFileSuffix(FileNameUtil.getSuffix(s3ObjectSummary.getKey()));
            respDto.setPath(Tools.removePrefix(Tools.removeFolderPath(s3ObjectSummary.getKey(), rootPath)));
            respDto.setOriginalPath(s3ObjectSummary.getKey());
            respDto.setSize(s3ObjectSummary.getSize());
            respDto.setFolder(false);
            respDto.setSizeFormatted(DataSizeUtil.format(s3ObjectSummary.getSize()));
            respDto.setCreateTime(s3ObjectSummary.getLastModified());
            fileList.add(respDto);
        }
    }

    static void listFolder(String rootPath, String path, List<String> commonPrefixes,
                           List<FileRespDTO> fileList) {
        listFolder(rootPath, path, null, commonPrefixes, fileList);
    }

    /**
     * 转换 文件夹列表
     */
    static void listFolder(String rootPath, String path, String storagePath, List<String> commonPrefixes,
        List<FileRespDTO> fileList) {
        for (String commonPrefix : commonPrefixes) {
            if (storagePath != null && commonPrefix.equals(storagePath)) {
                continue;
            }
            FileRespDTO respDto = new FileRespDTO();
            respDto.setName(Tools.getLastFolder(StringUtils.removeStart(commonPrefix, rootPath)));
            respDto.setDisplayName(respDto.getName());
            respDto.setPath(Tools.removePrefix(StringUtils.removeStart(commonPrefix, rootPath)));
            respDto.setOriginalPath(commonPrefix);
            respDto.setFolder(true);
            fileList.add(respDto);
        }
    }

    /**
     * 转换 文件夹列表(递归)
     */
    static List<FolderRespDTO> listFolderRecursively(String storePath, String path, List<S3ObjectSummary> objectSummaries) {

        Set<String> paths = new HashSet<>();
        for (S3ObjectSummary objectSummary : objectSummaries) {
            String middlePath = Tools.getMiddlePath(storePath, objectSummary.getKey());
            if (StringUtils.isBlank(middlePath)) {
                continue;
            }
            paths.add(middlePath);
        }

        FolderRespDTO root = new FolderRespDTO("", "");
        for (String p : paths) {
            addPathToTree(root, p);
        }
        return root.getChildren();
    }

    /**
     * 添加路径到树结构
     */
    private static void addPathToTree(FolderRespDTO root, String path) {
        if (path == null || path.isEmpty()) {return;}

        // 分割路径
        String[] parts = path.split("/");
        FolderRespDTO currentNode = root;
        StringBuilder currentPath = new StringBuilder();

        for (int i = 0; i < parts.length; i++) {
            String part = parts[i];
            if (part.isEmpty()) {
                continue;
            }

            // 构建当前完整路径
            if (currentPath.length() > 0) {
                currentPath.append("/");
            }
            currentPath.append(part);

            // 查找或创建节点
            FolderRespDTO child = currentNode.findChild(part);
            if (child == null) {
                child = new FolderRespDTO(part, Tools.appendSuffix(currentPath.toString()));
                currentNode.getChildren().add(child);
            }

            currentNode = child;
        }
    }
}
