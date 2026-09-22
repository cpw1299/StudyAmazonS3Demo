package com.yhcx.framework.file.core.dto;

import lombok.Data;

import java.util.ArrayList;
import java.util.List;

@Data
public class ArchiveResult {

    private List<FileUploadDTO> fileList = new ArrayList<>();

    private List<String> folderList = new ArrayList<>();

    public ArchiveResult() {
    }

    public ArchiveResult(List<FileUploadDTO> fileList, List<String> folderList) {
        this.fileList = fileList;
        this.folderList = folderList;
    }
}
