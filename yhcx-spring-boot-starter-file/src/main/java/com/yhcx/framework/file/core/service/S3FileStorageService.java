package com.yhcx.framework.file.core.service;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.PartETag;
import com.github.junrar.exception.RarException;
import com.yhcx.framework.common.pojo.PageResult;
import com.yhcx.framework.file.core.dto.FileDTO;
import com.yhcx.framework.file.core.dto.FileRespDTO;
import com.yhcx.framework.file.core.dto.FolderRespDTO;
import com.yhcx.module.infra.api.file.dto.FilePresignedUrlRespDTO;
import org.springframework.web.multipart.MultipartFile;

import javax.servlet.http.HttpServletResponse;
import javax.validation.constraints.NotBlank;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

/**
 * S3 客户端实现文件存储
 *
 * @author liuhm
 * @version 1.0.0
 * @create 2025/11/12 16:40
 **/
public interface S3FileStorageService {

    AmazonS3Client getS3Client();

    Object listObjectsV2(String bucketName, String rootPath, String path, boolean delimiter);

    Object getObject(String bucketName, String rootPath, String path);

    Object putObject(String bucketName, String rootPath, String path, Map<String, String> metadata);

    InputStream downloadAsStream(String bucket, String rootPath, String path) throws IOException;

    /**
     * 当前目录下的文件、文件夹列表
     *
     * @param bucketName 桶名称
     * @param rootPath   根目录
     * @param path       路径
     * @param metadata   是否返回元数据
     * @param pageNo     页码
     * @return 文件列表
     */
    PageResult<FileRespDTO> filePageList(@NotBlank String bucketName, String rootPath, String path, Boolean metadata, Integer pageNo);

    /**
     * 当前目录下的文件、文件夹列表
     *
     * @param bucketName 桶名称
     * @param rootPath   根目录
     * @param path       路径
     * @return 文件列表
     */
    List<FileRespDTO> fileListAll(@NotBlank String bucketName, String rootPath, String path);

    /**
     * 当前目录下的文件、文件夹列表
     *
     * @param bucketName 桶名称
     * @param rootPath   根目录
     * @param path       路径
     * @return 文件列表
     */
    List<FileRespDTO> fileListAllWithMetadata(@NotBlank String bucketName, String rootPath, String path, boolean metadata);

    /**
     * 当前目录下的所有文件
     *
     * @param bucketName 桶名称
     * @param rootPath   根目录
     * @param path       路径
     * @return 文件列表
     */
    List<FileRespDTO> fileDirectList(@NotBlank String bucketName, String rootPath, String path);

    /**
     * 当前目录下的文件夹列表
     *
     * @param bucketName 桶名称
     * @param rootPath   根目录
     * @param path       路径
     * @return 文件夹列表
     */
    List<FolderRespDTO> folderList(@NotBlank String bucketName, String rootPath, String path);

    /**
     * 当前目录下的文件夹列表
     *
     * @param bucketName 桶名称
     * @param rootPath   根目录
     * @param path       路径
     * @return 文件夹列表
     */
    List<FolderRespDTO> folderList(@NotBlank String bucketName, String rootPath, String path, Boolean metadata);

    /**
     * 当前目录下的文件、文件夹数量大小
     *
     * @param bucketName 桶名称
     * @param rootPath   根目录
     * @param path       路径
     * @return FileDTO
     */
    FileDTO fileCountSize(@NotBlank String bucketName, String rootPath, String path);

    /**
     * 上传文件
     *
     * @param bucketName 桶名称
     * @param rootPath   根目录
     * @param path       路径
     * @param file       MultipartFile
     * @return 文件路径
     */
    String upload(@NotBlank String bucketName, String rootPath, String path, MultipartFile file) throws IOException, RarException;


    /**
     * 文件解压并上传 解压包会存在特别大文件 比例：test/test.zip 100G  解压后的文件也可以存储大文件 100G
     * 解压包  把压缩包对象上传到目标桶，解压包对象解压并上传到目标桶 只解压ZIP
     * @param bucketName 桶名称
     * @param rootPath   根目录
     * @param extractPath 提取路径  解压包会存在特别大文件 比例：test/test.zip 100G  解压后的文件也可以存储大文件 100G
     * @param targetBucketName 目标桶名称
     * @param targetFolderPath 目标文件夹路径
     * @return 文件路径
     */
    String zipPathFileExtract(@NotBlank String bucketName, String rootPath, String extractPath,
                       String targetBucketName, String targetFolderPath);

    /**
     * 上传文件
     *
     * @param bucketName 桶名称
     * @param rootPath   根目录
     * @param path       路径
     * @param content    文件内容
     * @return 文件路径
     */
    String upload(@NotBlank String bucketName, String rootPath, String path, byte[] content);

    /**
     * 上传文件
     *
     * @param bucketName 桶名称
     * @param rootPath   根目录
     * @param file       MultipartFile
     */
    void uploadZip(@NotBlank String bucketName, String rootPath, String path, MultipartFile file) throws IOException, RarException;

    /**
     * 上传文件，直接返回访问地址
     *
     * @param bucketName 桶名称
     * @param rootPath   根目录
     * @param path       路径
     * @param content    文件内容
     * @return 文件路径
     */
    String uploadUrl(@NotBlank String bucketName, String rootPath, String path, byte[] content);

    /**
     * 预签名
     *
     * @param bucketName 桶名称
     * @param rootPath   根目录
     * @param path       路径
     * @return 预签名URL
     */
    FilePresignedUrlRespDTO getFilePresignedUrl(@NotBlank String bucketName, String rootPath, String path);

    /**
     * 预签名
     *
     * @param bucketName 桶名称
     * @param rootPath   根目录
     * @param path       路径
     * @return 预签名URL
     */
    FilePresignedUrlRespDTO putFilePresignedUrl(@NotBlank String bucketName, String rootPath, String path);

    /**
     * 初始化客户端直传的分片上传
     */
    String initiateMultipartUpload(@NotBlank String bucketName, String rootPath, String path, String contentType);

    /**
     * 接收应用服务器分片流并上传到 MinIO
     */
    PartETag uploadMultipartPart(@NotBlank String bucketName, String rootPath, String path,
                                 String uploadId, Integer partNumber, MultipartFile file) throws IOException;

    /**
     * 检查 MinIO 分片上传任务中是否存在指定分片
     */
    boolean checkUploadMultipartPart(@NotBlank String bucketName, String rootPath, String path,
                                     String uploadId, Integer partNumber);

    /**
     * 查询已上传分片，用于断点续传
     */
    List<PartETag> listMultipartUploadParts(@NotBlank String bucketName, String rootPath, String path, String uploadId);

    /**
     * 查询 MinIO 已上传分片并完成合并
     */
    String completeMultipartUpload(@NotBlank String bucketName, String rootPath, String path, String uploadId);

    /**
     * 取消分片上传并清理已上传分片
     */
    void abortMultipartUpload(@NotBlank String bucketName, String rootPath, String path, String uploadId);

    /**
     * 删除文件
     *
     * @param bucketName 桶名称
     * @param path       路径
     */
    void deleteDirectory(@NotBlank String bucketName, String rootPath, String path);

    /**
     * 删除文件
     *
     * @param bucketName 桶名称
     * @param path       路径
     */
    void delete(@NotBlank String bucketName, String rootPath, String path);

    /**
     * 获取文件内容
     *
     * @param bucketName 桶名称
     * @param path       路径
     * @return 文件内容
     */
    byte[] getContent(@NotBlank String bucketName, String rootPath, String path);

    /**
     * 文件夹文件下载
     *
     * @param bucketName 桶名称
     * @param zipName    zipName
     * @param rootPath   根目录
     * @param path       路径
     * @param response   response
     */
    void folderFileDownload(@NotBlank String bucketName, String zipName, String rootPath, String path, HttpServletResponse response);

    /**
     * 删除缓存
     *
     * @param bucketName 桶名称
     * @param path       路径
     */
    void deleteDavResourceFromCache(@NotBlank String bucketName, String path);

    /**
     * 删除缓存
     *
     * @param bucketName 桶名称
     * @param rootPath   根目录
     * @param path       路径
     */
    void deleteDavResourceFromCache(@NotBlank String bucketName, String rootPath, String path);

    /**
     * 创建目录
     *
     * @param bucketName 桶名称
     * @param rootPath   根目录
     * @param path       路径
     */
    void createDirectory(@NotBlank String bucketName, String rootPath, String path);

    /**
     * 创建目录
     *
     * @param bucketName 桶名称
     * @param rootPath   根目录
     * @param path       路径
     */
    void createDirectory(@NotBlank String bucketName, String rootPath, String path, Map<String, String> metadata);

    /**
     * 复制目录
     *
     * @param sourcePath 源路径
     * @param targetPath 目标路径
     */
    void copyDirectory(@NotBlank String bucketName, String sourcePath, String targetPath);

    /**
     * 从sourceBucketName目录下复制文件到targetBucketName目录下
     *
     * @param sourceBucketName 原桶名称
     * @param sourceFolderPath 【文件夹】源路径 案例：/folder1/test1
     * @param targetBucketName 目标桶名称
     * @param targetFolderPath 【文件夹】目标路径 案例：/folder2/test3
     */
    void copyDirectory(@NotBlank String sourceBucketName, String sourceFolderPath, String targetBucketName, String targetFolderPath);

    /**
     * 从sourceBucketName目录移动文件到targetBucketName目录下
     *
     * @param sourceBucketName 原桶名称
     * @param sourceFolderPath 【文件夹】源路径 案例：/folder1/test1
     * @param targetBucketName 目标桶名称
     * @param targetFolderPath 【文件夹】目标路径 案例：/folder2/test3
     */
    void moveDirectory(@NotBlank String sourceBucketName, String sourceFolderPath, String targetBucketName, String targetFolderPath);

    /**
     * 从sourceBucketName目录下复制文件到targetBucketName目录下
     *
     * @param sourceBucketName 原桶名称
     * @param sourceFolderPath 【文件夹】源路径 案例：/folder1/test1
     * @param targetBucketName 目标桶名称
     * @param targetZipPath    【文件夹】目标路径 案例：/folder2/test3.zip
     * @param skipFileType     @param skipFileType     需要跳过的文件后缀 如 [".tmp", ".log"]
     */
    void copyDirectoryToZip(@NotBlank String sourceBucketName, String sourceFolderPath, String targetBucketName, String targetZipPath, List<String> skipFileType);

    /**
     * 从sourceBucketName目录下移动文件到targetBucketName目录下
     *
     * @param sourceBucketName 原桶名称
     * @param sourceFolderPath 【文件夹】源路径 案例：/folder1/test1
     * @param targetBucketName 目标桶名称
     * @param targetZipPath    【文件夹】目标路径 案例：/folder2/test3.zip
     * @param skipFileType     @param skipFileType     需要跳过的文件后缀 如 [".tmp", ".log"]
     */
    void moveDirectoryToZip(@NotBlank String sourceBucketName, String sourceFolderPath, String targetBucketName, String targetZipPath, List<String> skipFileType);

    /**
     * 复制文件
     *
     * @param bucketName     桶名称
     * @param sourceFilePath 源文件路径
     * @param targetFilePath 目标文件路径
     */
    void copyFile(@NotBlank String bucketName, String sourceFilePath, String targetFilePath);

    /**
     * 复制文件
     *
     * @param sourceBucketName 原桶名称
     * @param sourceFilePath   源文件路径
     * @param targetBucketName 目标桶名称
     * @param targetFilePath   目标文件路径
     */
    void copyFile(@NotBlank String sourceBucketName, String sourceFilePath, String targetBucketName, String targetFilePath);

}
