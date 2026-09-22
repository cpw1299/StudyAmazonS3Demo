package com.yhcx.framework.file.core.service;

import com.amazonaws.services.s3.model.PartETag;
import java.io.IOException;
import java.util.List;
import org.springframework.web.multipart.MultipartFile;

/**
 * 本地实现文件存储
 *
 * @author liuhm
 * @version 1.0.0
 * @create 2025/11/12 16:40
 **/
public interface LocalFileStorageService {

    /**
     * 初始化客户端直传的分片上传
     */
    String initiateMultipartUpload(String rootPath, String path, String contentType);

    /**
     * 接收应用服务器分片流并上传到 MinIO
     */
    PartETag uploadMultipartPart(String rootPath, String path,
                                 String uploadId, Integer partNumber, MultipartFile file) throws IOException;

    /**
     * 检查 MinIO 分片上传任务中是否存在指定分片
     */
    boolean checkUploadMultipartPart(String rootPath, String path,
                                     String uploadId, Integer partNumber);

    /**
     * 查询已上传分片，用于断点续传
     */
    List<PartETag> listMultipartUploadParts(String rootPath, String path, String uploadId);

    /**
     * 查询 MinIO 已上传分片并完成合并
     */
    String completeMultipartUpload(String rootPath, String path, String uploadId);

    /**
     * 取消分片上传并清理已上传分片
     */
    void abortMultipartUpload(String rootPath, String path, String uploadId);

}
