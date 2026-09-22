package com.yhcx.framework.file.core.service;

import static com.yhcx.framework.common.exception.util.ServiceExceptionUtil.exception;
import static com.yhcx.framework.security.core.util.SecurityFrameworkUtils.getLoginUserId;

import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.core.io.FileUtil;
import cn.hutool.core.io.IoUtil;
import cn.hutool.core.io.unit.DataSizeUtil;
import cn.hutool.core.util.IdUtil;
import cn.hutool.core.util.ObjectUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.extra.spring.SpringUtil;
import com.github.sardine.DavResource;
import com.github.sardine.impl.SardineException;
import com.yhcx.framework.common.dto.WebdavDTO;
import com.yhcx.framework.common.enums.UserTypeEnum;
import com.yhcx.framework.common.exception.ErrorCode;
import com.yhcx.framework.common.pojo.PageResult;
import com.yhcx.framework.common.util.file.Tools;
import com.yhcx.framework.common.util.object.PageUtils;
import com.yhcx.framework.file.core.dto.FileDTO;
import com.yhcx.framework.file.core.dto.FileDownloadResult;
import com.yhcx.framework.file.core.dto.FilePercentDTO;
import com.yhcx.framework.file.core.dto.FileRespDTO;
import com.yhcx.framework.file.core.dto.FileShareRespDTO;
import com.yhcx.framework.file.core.dto.FolderRespDTO;
import com.yhcx.framework.file.core.dto.ProgressInputStream;
import com.yhcx.framework.webdav.config.WebdavProperties;
import com.yhcx.framework.webdav.core.WebdavClient;
import com.yhcx.framework.webdav.core.feign.client.WebdavFeignClient;
import com.yhcx.framework.webdav.core.feign.dto.WebdavShareDTO;
import com.yhcx.framework.webdav.core.feign.dto.WebdavSharesResponse;
import com.yhcx.framework.webdav.core.pool.WebdavConnectPool;
import com.yhcx.module.infra.api.websocket.WebSocketSenderApi;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import javax.annotation.Resource;
import javax.servlet.http.HttpServletResponse;
import javax.validation.constraints.NotBlank;
import javax.xml.namespace.QName;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Recover;
import org.springframework.retry.annotation.Retryable;
import org.springframework.web.client.HttpServerErrorException;

/**
 * FileStoragePlusUltraService
 *
 * @author liuhm
 * @version 1.0.0
 * @create 2025/11/12 15:36
 **/
@Slf4j
public class WebdavFileStoragePlusUltraServiceImpl implements FileStoragePlusUltraService{

    @Resource
    private WebdavProperties webdavProperties;
    @Resource
    private WebdavConnectPool webdavConnectPool;
    @Resource
    private StringRedisTemplate stringRedisTemplate;

    private final String WEBDAV_CACHE_PREFIX = "webdav:list:{}";

    public static final String HTTP = "http";

    private static final Integer PAGE_SIZE = 10;

    @Resource
    private WebSocketSenderApi webSocketSenderApi;

    @Resource
    private WebdavFeignClient webdavFeignClient;

    /**
     * 登录之后测试webdav连通情况
     * @param accessKey 云盘电子邮箱
     * @param accessSecret 云盘应用密码
     * @return Boolean
     */
    @Override
    public Boolean test(@NotBlank String accessKey, @NotBlank String accessSecret) {
        WebdavDTO webdavDTO = new WebdavDTO().setUsername(accessKey).setPassword(accessSecret);
        try (WebdavClient client = new WebdavClient(webdavDTO, webdavConnectPool)) {
            client.getSardine().exists(String.format(webdavProperties.getServer(), webdavDTO.getUsername()));
            return true;
        } catch (Exception e) {
            log.error("auth pan.crrcgc.cc fail, error:{}", e.getMessage());
            return false;
        }
    }

    /**
     * 递归创建目录
     */
    private void createDirectory(WebdavDTO webdavDTO ,String path) throws IOException {
        if (path == null) {
            return;
        }
        try (WebdavClient client = new WebdavClient(webdavDTO, webdavConnectPool)) {
            if (!existsDirectory(webdavDTO, client, path)) {
                createDirectory(webdavDTO, Tools.getParent(path));
                try {
                    client.getSardine().createDirectory(getUrl(webdavDTO ,path));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private boolean existsDirectory(WebdavDTO webdavDTO, WebdavClient client ,String path) throws IOException {
        if (webdavProperties.getServer().equals(path)) {
            return true;
        }
        try {
            if (StrUtil.containsAnyIgnoreCase(path, HTTP)) {
                return client.getSardine().exists(path);
            }
            return client.getSardine().exists(getUrl(webdavDTO ,path));
        } catch (SardineException e) {
            if (e.getStatusCode() == 404 || e.getStatusCode() == 409) {
                return false;
            } else {
                return true;
            }
        }
    }

    /**
     * 获取远程绝对路径
     */
    private String getUrl(WebdavDTO webdavDTO, String path) {
        return Tools.join(String.format(webdavProperties.getServer(), webdavDTO.getUsername()), (webdavProperties.getBasePath() + Tools.removePrefix(path))
            .replaceAll("//", "/").replaceAll(" ", "%20"));
    }

    /**
     * 获取远程绝对路径
     */
    private String getTempUrl(WebdavDTO webdavDTO, String path) {
        return Tools.join(String.format(webdavProperties.getServer(), webdavDTO.getUsername()),
            (Tools.removePrefix(path)).replaceAll("//", "/").replaceAll(" ", "%20"));
    }

    @Override
    public List<FileRespDTO> fileList(WebdavDTO webdavDTO, String relativeDatasetRootPath, @NotBlank String path) {
        String finalPath = Tools.join(relativeDatasetRootPath, path);
        List<FileRespDTO> fileRespDTOList = new ArrayList<>();

        List<FileRespDTO> list = getSelf().getDavResourceFromCache(webdavDTO, getUrl(webdavDTO, finalPath), path);

        if (list.size() <= 1) {
            return null;
        }
        list = CollectionUtil.sub(list, 1, list.size());

        for (FileRespDTO fileRespDTO : list) {
            fileRespDTO.setDisplayName(fileRespDTO.getName());
            fileRespDTOList.add(fileRespDTO);
        }
        return fileRespDTOList;
    }

    @Override
    public List<FileRespDTO> fileDirectList(WebdavDTO webdavDTO, String relativeDatasetRootPath, @NotBlank String path) {
        String finalPath = Tools.join(relativeDatasetRootPath, path);
        try (WebdavClient client = new WebdavClient(webdavDTO, webdavConnectPool)) {
            //  创建要获取的属性 (QName) 集合  oc:) 命名空间
            Set<QName> propsToFetch = new HashSet<>();
            final String ocNs = "http://owncloud.org/ns";
            propsToFetch.add(new QName(ocNs, "size", "oc"));
            List<DavResource> list = client.getSardine().list(getUrl(webdavDTO, finalPath),-1, propsToFetch);
            list = CollectionUtil.sub(list, 1, list.size());
            return list.stream().map(davResource -> getDirectFileRespDTO(davResource, relativeDatasetRootPath, path)).collect(
                Collectors.toList());
        } catch (Exception e) {
            log.error("获取文件报错：{}，error：{}", finalPath, e);
            return new ArrayList<>();
        }
    }

    @Override
    public FileDTO fileCountSize(WebdavDTO webdavDTO, String relativeDatasetRootPath, String path) {
        List<FileRespDTO> fileRespList = fileDirectList(webdavDTO, relativeDatasetRootPath, path);
        if (CollectionUtil.isEmpty(fileRespList)) {
            return new FileDTO(0L, 0L);
        }
        fileRespList = fileRespList.stream().filter(fileRespDTO ->
            !fileRespDTO.getFolder()).collect(Collectors.toList());
        Long fileCount  = (long) fileRespList.size();
        FileRespDTO fileRespDTO = fileRespList.get(0);
        return new FileDTO(fileCount, fileRespDTO.getSize());
    }

    @Override
    public PageResult<FileRespDTO> filePageList(WebdavDTO webdavDTO, String relativeDatasetRootPath, @NotBlank String path, Integer pageNo) {
        String finalPath = Tools.join(relativeDatasetRootPath, path);

        List<FileRespDTO> list = getSelf().getDavResourceFromCache(webdavDTO, getUrl(webdavDTO ,finalPath), path);
        int davResourceListSize = list.size();
        if (davResourceListSize >= 1) {
            list = list.subList(1, davResourceListSize);
        }
        List<FileRespDTO> pageList = PageUtils.getPageLimit(list, pageNo, PAGE_SIZE);

        List<FileRespDTO> fileRespDTOList = new ArrayList<>();
        for (FileRespDTO fileRespDTO : pageList) {
            fileRespDTO.setDisplayName(fileRespDTO.getName());
            fileRespDTOList.add(fileRespDTO);
        }
        return new PageResult<>(fileRespDTOList, (long) list.size());
    }

    @Override
    public List<FolderRespDTO> folderList(WebdavDTO webdavDTO, String relativeDatasetRootPath,
        @NotBlank String path, Integer level) {

        String storePath = Tools.join(relativeDatasetRootPath, path);
        try (WebdavClient webdavClient = new WebdavClient(webdavDTO, webdavConnectPool)) {
            return listFoldersRecursively(webdavDTO, webdavClient, storePath, "/", 1, level);
        }
    }

    private List<FolderRespDTO> listFoldersRecursively(WebdavDTO webdavDTO, WebdavClient webdavClient, String rootPath, String subPath, int currentLevel, int maxLevel) {
        List<FolderRespDTO> result = new ArrayList<>();
        try{
            List<DavResource> resources = webdavClient.getSardine().list(getUrl(webdavDTO, Tools.join(rootPath, subPath)));

            for (int i = 0; i < resources.size(); i++) {
                if (i == 0) {
                    continue;
                }
                DavResource resource = resources.get(i);
                if (!resource.isDirectory()) {
                    continue;
                }

                FolderRespDTO dto = new FolderRespDTO();
                dto.setName(resource.getName());
                dto.setPath(Tools.join(subPath, dto.getName()));

                // 控制递归深度
                if (currentLevel < maxLevel) {
                    dto.setChildren(listFoldersRecursively(webdavDTO ,webdavClient ,rootPath, Tools.join(subPath, dto.getName()), currentLevel + 1, maxLevel));
                }
                result.add(dto);
            }
        } catch (IOException e) {
            log.error("递归列表文件报错：{}，error：{}", rootPath, e.getMessage());
        }
        return result;
    }

    @Cacheable(value = "webdav#600s", key = "'list:'+#httpDatasetRootPath", unless = "#result == null || #result.size() == 0")
    public List<FileRespDTO> getDavResourceFromCache(WebdavDTO webdavDTO, String httpDatasetRootPath, String path) {
        try (WebdavClient client = new WebdavClient(webdavDTO, webdavConnectPool)) {
            List<DavResource> list = client.getSardine().list(httpDatasetRootPath);
            return list.stream().map(davResource -> getFileRespDTO(davResource, path))
                .collect(Collectors.toList());
        } catch (Exception e) {
            log.error("获取文件报错：{}，error：{}", httpDatasetRootPath, e);
            return new ArrayList<>();
        }
    }

      private FileRespDTO getDirectFileRespDTO(DavResource davResource, String relativeDatasetRootPath ,String path) {
        String[] resourcePath = davResource.getPath().split("/");
        String fileName = resourcePath[resourcePath.length - 1];
        FileRespDTO fileRespDTO = new FileRespDTO();
        fileRespDTO.setName(davResource.getName());
        fileRespDTO.setDisplayName(davResource.getName());
        fileRespDTO.setFolder(davResource.isDirectory());
        fileRespDTO.setPath(Tools.join(path, StrUtil.subAfter(davResource.getPath(), path, true)));
        fileRespDTO.setOriginalPath(Tools.join(relativeDatasetRootPath, fileRespDTO.getPath()));
        fileRespDTO.setSize(davResource.getContentLength());
        fileRespDTO.setFileSuffix(FileUtil.getSuffix(davResource.getName()));
        fileRespDTO.setCreateTime(davResource.getModified());
        if (davResource.isDirectory()) {
            fileRespDTO.setSizeFormatted("--");
            Map<String, String> customProps = davResource.getCustomProps();
            fileRespDTO.setSize(customProps.get("size") == null ? 0 : Long.parseLong(customProps.get("size")));
        }else{
            fileRespDTO.setSizeFormatted(DataSizeUtil.format(davResource.getContentLength()));
        }
        return fileRespDTO;
    }

    private FileRespDTO getFileRespDTO(DavResource davResource, String path) {
        String[] resourcePath = davResource.getPath().split("/");
        String fileName = resourcePath[resourcePath.length - 1];
        FileRespDTO fileRespDTO = new FileRespDTO();
        fileRespDTO.setName(davResource.getName());
        fileRespDTO.setDisplayName(davResource.getName());
        fileRespDTO.setFolder(davResource.isDirectory());
        fileRespDTO.setPath(Tools.join(path ,fileName));
        fileRespDTO.setSize(davResource.getContentLength());
        fileRespDTO.setFileSuffix(FileUtil.getSuffix(davResource.getName()));
        fileRespDTO.setCreateTime(davResource.getModified());
        if (davResource.isDirectory()) {
            fileRespDTO.setSizeFormatted("--");
            Map<String, String> customProps = davResource.getCustomProps();
            fileRespDTO.setSize(customProps.get("size") == null ? 0 : Long.parseLong(customProps.get("size")));
        }else{
            fileRespDTO.setSizeFormatted(DataSizeUtil.format(davResource.getContentLength()));
        }
        return fileRespDTO;
    }

    @Override
    @Retryable(value = HttpServerErrorException.class, maxAttempts = 3, backoff = @Backoff(delay = 5000L, multiplier = 1.5))
    public String upload(WebdavDTO webdavDTO, byte[] content, @NotBlank String path) {
        try (WebdavClient client = new WebdavClient(webdavDTO, webdavConnectPool)) {
            if (path.contains("/") || path.contains("\\")) {
                String finalPath = Tools.getParent(path);
                createDirectory(webdavDTO, finalPath);
            }
            client.getSardine().put(getUrl(webdavDTO, path), content);

            return getUrl(webdavDTO, path);
        } catch (Exception e) {
            log.error("上传文件报错：{}", e.getMessage());
            throw new HttpServerErrorException(HttpStatus.INTERNAL_SERVER_ERROR, e.getMessage());
        }
    }

    @Override
    public String uploadUrl(WebdavDTO webdavDTO, byte[] content, String path) {
        // 上传
        upload(webdavDTO, content, path);
        // 共享
        return shareResource(webdavDTO, path);
    }

    @Recover
    public void uploadRecover(Exception e) {
        // 回调方法,业务逻辑处理
        throw new HttpServerErrorException(HttpStatus.INTERNAL_SERVER_ERROR, e.getMessage());
    }

    @Override
    public String uploadLargeFile(WebdavDTO webdavDTO, String uid, byte[] content, @NotBlank String path) {
        final String tId = ObjectUtil.defaultIfNull(uid, IdUtil.fastSimpleUUID());
        // 创建进度监听器
        ProgressInputStream.ProgressListener listener = (bytes, total, percent) -> {
            printProgressBar(path, percent, bytes, total);
            webSocketSenderApi
                .sendObject(UserTypeEnum.ADMIN.getValue(), getLoginUserId(),
                    tId,
                    new FilePercentDTO(tId, FileUtil.getName(path),  percent));
        };

        // 使用 try-with-resources 确保流被关闭
        try (WebdavClient client = new WebdavClient(webdavDTO, webdavConnectPool)) {
            if (path.contains("/") || path.contains("\\")) {
                String finalPath = Tools.getParent(path);
                createDirectory(webdavDTO, finalPath);
            }

            ProgressInputStream progressStream = new ProgressInputStream(IoUtil.toStream(content), content.length, listener);
            // 使用流式 put 方法
            client.getSardine().put(getUrl(webdavDTO, path), progressStream, "application/octet-stream");
            log.debug("{}：上传成功",  path);
            return getUrl(webdavDTO, path);
        } catch (Exception e) {
            log.error("上传文件报错：{}", e.getMessage());
            throw exception(new ErrorCode(HttpStatus.INTERNAL_SERVER_ERROR.value(), "文件上传失败"));
        }
    }

    // 辅助方法：打印进度条
    private static void printProgressBar(String path, int percent, long bytes, long total) {
        log.debug("文件上传-{}: {}% {} / {}", path, percent, bytes, total);
    }

    @Override
    public byte[] getContent(WebdavDTO webdavDTO, String path) {
        try (WebdavClient client = new WebdavClient(webdavDTO, webdavConnectPool)) {
            InputStream inputStream = client.getSardine().get(getUrl(webdavDTO, path));
            return inputStream.readAllBytes();
        } catch (Exception e) {
            throw new RuntimeException("获取文件内容异常:" + e.getMessage());
        }
    }

    @Override
    public byte[] getContentPackTargetFoldersRecursively(String repositoryPath, List<String> targetFolders) {
        return new byte[0];
    }

    @Override
    public InputStream getInputStream(WebdavDTO webdavDTO, String path) {
        try (WebdavClient client = new WebdavClient(webdavDTO, webdavConnectPool)) {
            return client.getSardine().get(getUrl(webdavDTO, path));
        } catch (Exception e) {
            throw new RuntimeException("获取文件输入流异常:" + e.getMessage());
        }
    }

    @Override
    public void copyDirectory(WebdavDTO webdavDTO ,String sourcePath, String targetPath) {
        try (WebdavClient client = new WebdavClient(webdavDTO, webdavConnectPool)) {
            // 判断是否存在
            if (!existsDirectory(webdavDTO ,client, targetPath)) {
                createDirectory(webdavDTO, targetPath);
            }
            client.getSardine().copy(getUrl(webdavDTO, sourcePath), getUrl(webdavDTO, targetPath));
        } catch (Exception e) {
            throw new RuntimeException("复制文件异常:" + e.getMessage());
        }
    }

    @Override
    public void copyDirectoryToDataset(WebdavDTO webdavDTO, String sourcePath, String targetPath) {
        try (WebdavClient client = new WebdavClient(webdavDTO, webdavConnectPool)) {
            // 判断是否存在
            if (!existsDirectory(webdavDTO ,client, targetPath)) {
                createDirectory(webdavDTO, targetPath);
            }
            client.getSardine().copy(getUrl(webdavDTO, sourcePath), getUrl(webdavDTO, targetPath));
        } catch (Exception e) {
            throw new RuntimeException("复制文件异常:" + e.getMessage());
        }
    }

    @Override
    public void copyZipToDataset(WebdavDTO webdavDTO, String sourceZipUrl, String targetPath) {

    }

    @Override
    public void copyZipBucketFilePathToBucketTargetPath(WebdavDTO webdavDTO, String sourceBucketName, String sourceZipUrl, String targetBucketName, String targetFolderPath) {

    }

    @Override
    public void copyMaasDirectoryToDataset(WebdavDTO webdavDTO, String sourcePath, String targetPath) {

    }

    @Override
    public void copyFolderBucketFilePathToBucketTargetPath(WebdavDTO webdavDTO, String sourceBucketName, String sourceFolderPath, String targetBucketName, String targetFolderPath) {

    }

    @Override
    public void copyFile(String sourceFilePath, String targetFilePath) {
        return;
    }

    @Override
    public void copyFileBucketFilePathToBucketTargetPath(WebdavDTO webdavDTO, String sourceBucketName, String sourceFilePath, String targetBucketName, String targetFilePath) {

    }

    @Override
    public void moveDirectory(WebdavDTO webdavDTO ,String sourcePath, String targetPath) {
        try (WebdavClient client = new WebdavClient(webdavDTO, webdavConnectPool)) {
            // 判断是否存在
            if (!existsDirectory(webdavDTO, client, targetPath)) {
                createDirectory(webdavDTO, targetPath);
            }
            client.getSardine().move(getTempUrl(webdavDTO, sourcePath), getUrl(webdavDTO, targetPath));
        } catch (Exception e) {
            throw new RuntimeException("移动文件异常:" + e.getMessage());
        }
    }

    @Override
    public void delete(WebdavDTO webdavDTO ,@NotBlank String path) {
        try (WebdavClient client = new WebdavClient(webdavDTO, webdavConnectPool)) {
            client.getSardine().delete(getUrl(webdavDTO, path));
        } catch (Exception e) {
            log.error("删除文件报错：{}，error：{}", path, e.getMessage());
        }
    }

    @Override
    public void deleteDavResourceFromCache(WebdavDTO webdavDTO, String relativeDatasetRootPath, String path) {
        String storePath = null;
        if (StringUtils.isBlank(path)) {
            storePath = relativeDatasetRootPath;
        } else {
            storePath = Tools.join(relativeDatasetRootPath, path);
        }

        // 通过判断是否有文件后续判断是文件，还是文件夹
        String fileSuffix = FileUtil.getSuffix(storePath);
        // 如果没有后续名，则是文件夹
        if (StringUtils.isNotBlank(fileSuffix)) {
            // 判断 path 是否带文件名后缀，如果是截断
            storePath = Tools.getParent(storePath);
        }
        stringRedisTemplate.delete(StrUtil.format(WEBDAV_CACHE_PREFIX, getUrl(webdavDTO, storePath)));
    }

    @Override
    public String shareResource(WebdavDTO webdavDTO, String path) {
        try {
            WebdavSharesResponse response = webdavFeignClient.shares(
                WebdavShareDTO.buildImage(Tools.join(webdavProperties.getBasePath(), path)));
            if (response.isSuccess()) {
                return response.getPreviewUrl();
            } else {
                return "";
            }
        } catch (Exception e) {
            log.error("分享文件报错：{}，error：{}", path, e.getCause());
            return "";
        }
    }

    @Override
    public FileShareRespDTO shareFileResource(WebdavDTO webdavDTO, String path) {
        try {
            WebdavSharesResponse response = webdavFeignClient.shares(WebdavShareDTO.buildImage(Tools.join(webdavProperties.getBasePath(), path)));
            if (response.isSuccess()) {
                FileShareRespDTO fileShareRespDTO = new FileShareRespDTO();
                fileShareRespDTO.setName(response.getName());
                fileShareRespDTO.setSharePath(response.getDownloadUrl(webdavProperties));
                return fileShareRespDTO;
            } else {
                return new FileShareRespDTO();
            }
        } catch (Exception e) {
            log.error("分享文件报错：{}，error：{}", path, e.getCause());
            return new FileShareRespDTO();
        }
    }

    @Override
    public String shareResourceToGroup(String shareToAccessKey, String path) {
        try {
            WebdavSharesResponse response = webdavFeignClient.shares(
                WebdavShareDTO.buildFile(shareToAccessKey, Tools.join(webdavProperties.getBasePath(), path)));
            if (response.isSuccess()) {
                return response.getPreviewUrl();
            } else {
                return "";
            }
        } catch (Exception e) {
            log.error("分享文件报错：{}，error：{}", path, e.getMessage());
            return "";
        }
    }

    @Override
    public byte[] folderFileDownload(WebdavDTO webdavDTO, String zipName, String relativeDatasetRootPath, String path) {
        List<java.util.concurrent.Future<FileDownloadResult>> futures = new ArrayList<>();
        // 线程池并发下载
        ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 2);

        // 获取所有文件
        List<FileRespDTO> allFiles = fileDirectList(webdavDTO, relativeDatasetRootPath, path);
        if (allFiles.isEmpty()) {
            return null;
        }

        for (FileRespDTO file : allFiles) {
            if (file.getFolder()) {
                continue;
            }
            futures.add(executor.submit(() -> {
                try {
                    byte[] content = getContent(webdavDTO, file.getOriginalPath());
                    return new FileDownloadResult(file, content, null);
                } catch (Exception e) {
                    log.error("下载文件失败: {}，error: {}", file.getOriginalPath(), e.getMessage());
                    return new FileDownloadResult(file, null, e);
                }
            }));
        }

        // 顺序写入zip
        String zipPath = Tools.join(System.getProperty("java.io.tmpdir"), zipName);
        try (FileOutputStream fos = new FileOutputStream(zipPath); ZipOutputStream zos = new ZipOutputStream(fos)) {
            byte[] buffer = new byte[8192];
            for (int i = 0; i < futures.size(); i++) {
                FileDownloadResult result;
                try {
                    result = futures.get(i).get();
                } catch (Exception e) {
                    log.error("获取下载结果失败: {}", e.getMessage());
                    continue;
                }
                if (result.getContent() == null) continue;

                zos.putNextEntry(new ZipEntry(StrUtil.subAfter(result.getFile().getPath(), path, true)));
                try (InputStream is = new ByteArrayInputStream(result.getContent())) {
                    int len;
                    while ((len = is.read(buffer)) > 0) {
                        zos.write(buffer, 0, len);
                    }
                } catch (Exception e) {
                    log.error("写入zip文件失败: {}", e.getMessage());
                    continue;
                }
                zos.closeEntry();
            }
        } catch (Exception e) {
            log.error("压缩文件失败: {}", e.getMessage());
            return null;
        } finally {
            executor.shutdown();
        }
        return FileUtil.readBytes(zipPath);
    }

    /**
     * 1. 获取文件列表
     * 2. 将文件列表转换为zip压缩包
     * 3. 将压缩包通过流式下载
     */
    @Override
    public void folderFileDownload(WebdavDTO webdavDTO, String zipName, String relativeDatasetRootPath, String path, HttpServletResponse response) {
        List<Future<FileDownloadResult>> futures = new ArrayList<>();
        // 线程池并发下载
        ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 2);

        // 获取所有文件
        List<FileRespDTO> allFiles = fileDirectList(webdavDTO, relativeDatasetRootPath, path);
        if (allFiles.isEmpty()) {
            throw new RuntimeException("数据集文件列表为空");
        }
        response.setContentType("application/zip");
        String zipFileName = zipName + ".zip";
        response.setHeader("Content-Disposition", "attachment; filename=\"" + zipFileName + "\"");

        for (FileRespDTO file : allFiles) {
            if (file.getFolder()) {
                continue;
            }
            futures.add(executor.submit(() -> {
                try {
                    byte[] content = getContent(webdavDTO, file.getOriginalPath());
                    return new FileDownloadResult(file, content, null);
                } catch (Exception e) {
                    log.error("下载文件失败: {}，error: {}", file.getOriginalPath(), e.getMessage());
                    return new FileDownloadResult(file, null, e);
                }
            }));
        }

        // 顺序写入zip
        try (ZipOutputStream zos = new ZipOutputStream(response.getOutputStream())) {
            byte[] buffer = new byte[8192];
            for (int i = 0; i < futures.size(); i++) {
                FileDownloadResult result;
                try {
                    result = futures.get(i).get();
                } catch (Exception e) {
                    log.error("获取下载结果失败: {}", e.getMessage());
                    continue;
                }
                if (result.getContent() == null) continue;

                zos.putNextEntry(new ZipEntry(StrUtil.subAfter(result.getFile().getPath(), path, true)));
                try (InputStream is = new ByteArrayInputStream(result.getContent())) {
                    int len;
                    while ((len = is.read(buffer)) > 0) {
                        zos.write(buffer, 0, len);
                    }
                } catch (Exception e) {
                    log.error("写入zip文件失败: {}", e.getMessage());
                    continue;
                }
                zos.closeEntry();
            }
            zos.finish();
        } catch (Exception e) {
            log.error("压缩文件失败: {}", e.getMessage());
            throw new RuntimeException("压缩文件失败");
        } finally {
            executor.shutdown();
        }
    }

    public WebdavFileStoragePlusUltraServiceImpl getSelf() {
        return SpringUtil.getBean(getClass());
    }

    @Override
    public void getTargetFoldersStream(String repositoryPath, List<String> targetFolders, OutputStream outputStream) {
    }

}
