package com.yhcx.framework.file.util;

import com.github.junrar.Archive;
import com.github.junrar.exception.RarException;
import com.github.junrar.exception.UnsupportedRarV5Exception;
import com.github.junrar.rarfile.FileHeader;
import com.yhcx.framework.file.core.dto.ArchiveResult;
import com.yhcx.framework.file.core.dto.FileUploadDTO;
import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipArchiveInputStream;
import org.springframework.web.multipart.MultipartFile;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class ArchiveUtils {

    public static final SecurityManager securityManager = new SecurityManager();

    public static ArchiveResult readArchive(MultipartFile file)
            throws RarException, IOException {

        String filename = file.getOriginalFilename();

        if (filename == null) {
            throw new IllegalArgumentException("文件名不能为空");
        }

        String lowerName = filename.toLowerCase();
        List<String> folderList = new ArrayList<>();
        List<FileUploadDTO> fileList = new ArrayList<>();

        if (lowerName.endsWith(".zip")) {

            readZip(file, fileList, folderList);

        } else if (lowerName.endsWith(".rar")) {

            readRar(file, fileList, folderList);

        } else {

            throw new IllegalArgumentException("仅支持 zip 或 rar 文件");
        }

        return new ArchiveResult(fileList, folderList);
    }

    public static void readZip(
            MultipartFile file,
            List<FileUploadDTO> fileList,
            List<String> folderList) throws IOException {
        fileList.clear();
        folderList.clear();
        try (InputStream inputStream = file.getInputStream();
             // 兼容GBK/UTF8双编码，解决中文文件名乱码&解码异常
             ZipArchiveInputStream zis = new ZipArchiveInputStream(inputStream, "GBK", true)) {

            ZipArchiveEntry entry;
            while ((entry = zis.getNextZipEntry()) != null) {
                String entryName = entry.getName();
                // 路径穿透防护
                if (entryName.contains("..")) {
                    continue;
                }
                if (entry.isDirectory()) {
                    String dirPath = entryName.endsWith("/") ? entryName.substring(0, entryName.length() - 1) : entryName;
                    folderList.add(dirPath);
                    continue;
                }
                Path entryPath = Paths.get(entryName);
                FileUploadDTO dto = new FileUploadDTO();
                dto.setFilePath(entryName);
                dto.setFileName(entryPath.getFileName().toString());

                try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
                    byte[] buffer = new byte[8192];
                    int len;
                    while ((len = zis.read(buffer)) != -1) {
                        bos.write(buffer, 0, len);
                    }
                    byte[] bytes = bos.toByteArray();
                    dto.setContent(bytes);
                    dto.setSize(bytes.length);
                }
                // mime探测容错
                String mimeType = null;
                try {
                    mimeType = Files.probeContentType(entryPath);
                } catch (Exception ignored) {
                }
                dto.setMimeType(mimeType);
                fileList.add(dto);
            }
        }
    }

    public static void readRar(
            MultipartFile file,
            List<FileUploadDTO> fileList,
            List<String> folderList) throws RarException, IOException {

        File temp = File.createTempFile("upload", ".rar");
        file.transferTo(temp);

        try (Archive archive = new Archive(temp)) {

            FileHeader header;

            while ((header = archive.nextFileHeader()) != null) {

                String path = header.getFileName();

                if (header.isDirectory()) {
                    folderList.add(path);
                    continue;
                }

                ByteArrayOutputStream baos = new ByteArrayOutputStream();

                archive.extractFile(header, baos);

                byte[] bytes = baos.toByteArray();

                FileUploadDTO dto = new FileUploadDTO();

                dto.setFilePath(path);
                dto.setFileName(Paths.get(path).getFileName().toString());
                dto.setContent(bytes);
                dto.setSize(bytes.length);
                dto.setMimeType(
                        Files.probeContentType(
                                Paths.get(dto.getFileName())));

                fileList.add(dto);
            }
        } catch (UnsupportedRarV5Exception e) {
            // 捕获 RAR5 不支持的异常，返回明确提示
            throw new IllegalArgumentException("暂不支持 RAR5 格式的压缩文件，请使用 ZIP 格式或 RAR4 格式");
        } catch (RarException e) {
            throw new IllegalArgumentException("RAR文件解析异常：" + e.getMessage());
        } finally {
            temp.delete();
            securityManager.checkDelete(temp.getName());
        }
    }

    public static void read7z(
            MultipartFile file,
            List<FileUploadDTO> fileList,
            List<String> folderList) throws IOException {

    }
}
