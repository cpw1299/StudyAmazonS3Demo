package com.yhcx.framework.file.core.constant;


import com.yhcx.framework.common.exception.ErrorCode;

/**
 * 错误码常量 100 901 001: 模块 功能 错误码
 *
 * @author 崔平卫
 * @version 1.0.0
 * @create 2024/7/3 20:30
 **/
public interface ErrorCodeConstant {

    // ========== 附件 100001001 ==========
    ErrorCode FILE_NOT_EXISTS = new ErrorCode(100901001, "文件不存在");
    ErrorCode FILE_UPLOAD_ERROR = new ErrorCode(100901002, "文件上传失败");
    ErrorCode FILE_DOWNLOAD_ERROR = new ErrorCode(100901003, "文件下载失败");
    ErrorCode FILE_FILE_SUFFIX_ERROR = new ErrorCode(100901004, "文件后缀名校验失败");
    ErrorCode FILE_IMAGE_FILE_SUFFIX_ERROR = new ErrorCode(100901004, "图片校验失败");

}
