package com.yhcx.framework.file.core.service;

import com.yhcx.framework.common.dto.WebdavDTO;
import com.yhcx.framework.common.pojo.PageResult;
import com.yhcx.framework.file.core.dto.FileDTO;
import com.yhcx.framework.file.core.dto.FileRespDTO;
import com.yhcx.framework.file.core.dto.FileShareRespDTO;
import com.yhcx.framework.file.core.dto.FolderRespDTO;
import com.yhcx.module.infra.api.file.dto.FilePresignedUrlRespDTO;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import javax.servlet.http.HttpServletResponse;
import javax.validation.constraints.NotBlank;

/**
 * FileStoragePlusUltraService
 *
 * @author liuhm
 * @version 1.0.0
 * @create 2025/11/12 16:40
 **/
public interface FileStoragePlusUltraService {

    /**
     * 测试
     *
     * @param accessKey    accessKey
     * @param accessSecret accessSecret
     * @return 测试结果
     */
    default Boolean test(@NotBlank String accessKey, @NotBlank String accessSecret) {
        return true;
    }

    /**
     * 当前目录下的文件、文件夹列表
     *
     * @param webdavDTO               webdavDTO
     * @param relativeDatasetRootPath 根目录
     * @param path                    路径
     * @return 文件列表
     */
    List<FileRespDTO> fileList(WebdavDTO webdavDTO, String relativeDatasetRootPath,
        @NotBlank String path);

    /**
     * 当前目录下的所有文件、文件夹列表
     *
     * @param webdavDTO               webdavDTO
     * @param relativeDatasetRootPath 根目录
     * @param path                    路径
     * @return 文件列表
     */
    List<FileRespDTO> fileDirectList(WebdavDTO webdavDTO, String relativeDatasetRootPath,
        @NotBlank String path);

    /**
     * 当前目录下的文件、文件夹数量大小
     *
     * @param webdavDTO               webdavDTO
     * @param relativeDatasetRootPath 根目录
     * @param path                    路径
     * @return FileDTO
     */
    FileDTO fileCountSize(WebdavDTO webdavDTO, String relativeDatasetRootPath,
        @NotBlank String path);

    /**
     * 当前目录下的文件、文件夹列表
     *
     * @param webdavDTO               webdavDTO
     * @param relativeDatasetRootPath 根目录
     * @param path                    路径
     * @param pageNo                  页码
     * @return 文件列表
     */
    PageResult<FileRespDTO> filePageList(WebdavDTO webdavDTO,
        String relativeDatasetRootPath, @NotBlank String path, Integer pageNo);

    /**
     * 当前目录下的文件夹列表
     *
     * @param webdavDTO               webdavDTO
     * @param relativeDatasetRootPath 根目录
     * @param path                    路径
     * @param level                   层级
     * @return 文件夹列表
     */
    List<FolderRespDTO> folderList(WebdavDTO webdavDTO, String relativeDatasetRootPath,
        @NotBlank String path, Integer level);

    /**
     * 上传文件
     *
     * @param webdavDTO webdavDTO
     * @param path      路径
     * @param content   文件内容
     * @return 文件路径
     */
    String upload(WebdavDTO webdavDTO, byte[] content, @NotBlank String path);

    /**
     * 上传文件，直接返回访问地址
     *
     * @param webdavDTO webdavDTO
     * @param path      路径
     * @param content   文件内容
     * @return 文件路径
     */
    String uploadUrl(WebdavDTO webdavDTO, byte[] content, @NotBlank String path);

    /**
     * 上传文件，大文件流式上传
     *
     * @param webdavDTO webdavDTO
     * @param uid       uid
     * @param content   文件内容
     * @param path      路径
     * @return 文件路径
     */
    default String uploadLargeFile(WebdavDTO webdavDTO, String uid, byte[] content,
        @NotBlank String path) {
        return upload(webdavDTO, content, path);
    }

    /**
     * 删除文件
     *
     * @param webdavDTO webdavDTO
     * @param path      路径
     */
    void delete(WebdavDTO webdavDTO, @NotBlank String path);

    /**
     * 获取文件内容
     *
     * @param webdavDTO webdavDTO
     * @param path      路径
     * @return 文件内容
     */
    byte[] getContent(WebdavDTO webdavDTO, String path);

    /**
     * 包getTargetFolders返回的目标文件夹及内部所有子文件夹/文件
     *
     * @param repositoryPath 路径
     * @param targetFolders  指定文件夹 如果为空 则获取repositoryPath下的返回的目标文件夹及内部所有子文件夹/文件
     * @return 文件内容
     */
    byte[] getContentPackTargetFoldersRecursively(String repositoryPath, List<String> targetFolders);


    /**
     * 获取文件输入流
     *
     * @param webdavDTO webdavDTO
     * @param path      路径
     * @return 文件输入流
     */
    InputStream getInputStream(WebdavDTO webdavDTO, String path);

    /**
     * 共享文件资源
     *
     * @param webdavDTO webdavDTO
     * @param path      路径
     * @return 文件大小
     */
    default String shareResource(WebdavDTO webdavDTO, String path) {
        return null;
    }

    /**
     * 共享文件资源
     *
     * @param webdavDTO webdavDTO
     * @param path      路径
     * @return 文件分享资源
     */
    default FileShareRespDTO shareFileResource(WebdavDTO webdavDTO, String path) {
        return null;
    }

    /**
     * 文件夹文件下载
     *
     * @param webdavDTO               webdavDTO
     * @param zipName                 zipName
     * @param relativeDatasetRootPath 根目录
     * @param path                    路径
     * @return 文件夹文件下载
     */
    default byte[] folderFileDownload(WebdavDTO webdavDTO, String zipName,
        String relativeDatasetRootPath, String path) {
        return null;
    }

    /**
     * 文件夹文件下载
     *
     * @param webdavDTO               webdavDTO
     * @param zipName                 zipName
     * @param relativeDatasetRootPath 根目录
     * @param path                    路径
     * @param response                response
     */
    void folderFileDownload(WebdavDTO webdavDTO, String zipName, String relativeDatasetRootPath,
        String path, HttpServletResponse response);

    /**
     * 删除缓存
     *
     * @param webdavDTO               webdavDTO
     * @param relativeDatasetRootPath 根目录
     * @param path                    路径
     */
    void deleteDavResourceFromCache(WebdavDTO webdavDTO, String relativeDatasetRootPath,
        String path);

    /**
     * 创建目录
     *
     * @param webdavDTO               webdavDTO
     * @param relativeDatasetRootPath 根目录
     * @param path                    路径
     */
    default void createDirectory(WebdavDTO webdavDTO, String relativeDatasetRootPath, String path){}

    /**
     * 复制目录
     *
     * @param sourcePath 源路径
     * @param targetPath 目标路径
     */
    void copyDirectory(WebdavDTO webdavDTO, String sourcePath, String targetPath);

    /**
     * 复制目录  从【非】dataset目录下复制文件到dataset目录下
     * @param sourcePath 源路径
     * @param targetPath 目标路径
     */
    void copyDirectoryToDataset(WebdavDTO webdavDTO, String sourcePath, String targetPath);

    /**
     * 从【非】dataset zip复制文件到dataset目录下，并进行解压
     * @param sourceZipUrl 源路径
     * @param targetPath 目标路径
     */
    void copyZipToDataset(WebdavDTO webdavDTO, String sourceZipUrl, String targetPath);

    /**
     * 从bucket复制zip文件到dataset目录下，并进行解压
     * @param webdavDTO
     * @param sourceBucketName
     * @param sourceZipUrl
     * @param targetBucketName
     * @param targetFolderPath
     */
    void copyZipBucketFilePathToBucketTargetPath(WebdavDTO webdavDTO, String sourceBucketName, String sourceZipUrl, String targetBucketName, String targetFolderPath);

    /**
     * 从maas平台复制目录  从【非】dataset目录下复制文件到dataset目录下
     * @param sourcePath 源路径
     * @param targetPath 目标路径
     */
    void copyMaasDirectoryToDataset(WebdavDTO webdavDTO, String sourcePath, String targetPath);

    /**
     * 从sourceBucketName目录下复制文件到targetBucketName目录下
     * @param webdavDTO
     * @param sourceBucketName
     * @param sourceFolderPath 【文件夹】源路径 案例：/folder1/test1
     * @param targetBucketName
     * @param targetFolderPath 【文件夹】目标路径 案例：/folder2/test3
     */
    void copyFolderBucketFilePathToBucketTargetPath(WebdavDTO webdavDTO,String sourceBucketName,String sourceFolderPath,String targetBucketName,String targetFolderPath);

    /**
     * 复制文件
     *
     * @param sourceFilePath 源文件路径
     * @param targetFilePath 目标文件路径
     */
    void copyFile(String sourceFilePath, String targetFilePath);

    /**
     * 从sourceBucketName复制文件到 targetBucketName 目录下
     * @param webdavDTO
     * @param sourceBucketName
     * @param sourceFilePath 【文件】源路径 案例：/folder1/test1.txt
     * @param targetBucketName
     * @param targetFilePath 【文件】源路径 案例：/folder2/test3.txt
     */
    void copyFileBucketFilePathToBucketTargetPath(WebdavDTO webdavDTO, String sourceBucketName, String sourceFilePath, String targetBucketName, String targetFilePath);

    /**
     * 获取文件预签名URL
     *
     * @param webdavDTO               webdavDTO
     * @param relativeDatasetRootPath 根目录
     * @param path                    路径
     * @return 文件预签名URL
     */
    default FilePresignedUrlRespDTO getFilePresignedUrl(WebdavDTO webdavDTO,
        String relativeDatasetRootPath, String path) {
        return null;
    }

    /**
     * 分享文件资源
     *
     * @param accessKey               accessKey
     * @param datasetRootPath         根目录
     * @return 文件分享资源
     */
    default String shareResourceToGroup(String accessKey, String datasetRootPath){
        return null;
    }

    /**
     * 移动目录
     *
     * @param webdavDTO               webdavDTO
     * @param sourcePath              源路径
     * @param targetPath              目标路径
     */
    default void moveDirectory(WebdavDTO webdavDTO ,String sourcePath, String targetPath) {
        return;
    }

    /**
     * 获取文件输入流
     *
     * @param repositoryPath          仓库路径
     * @param targetFolders           目标文件夹
     * @param outputStream            输出流
     * @return 文件输入流
     */
    void getTargetFoldersStream(String repositoryPath, List<String> targetFolders, OutputStream outputStream);
}
