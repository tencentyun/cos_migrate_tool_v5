package com.qcloud.cos_migrate_tool.config;

import java.io.File;
import java.util.regex.Pattern;

import com.qcloud.cos_migrate_tool.utils.PathUtils;
import com.qcloud.cos_migrate_tool.utils.SystemUtils;

public class CopyBucketConfig extends CommonConfig {
    private String srcRegion;
    private String srcBucket;
    private String srcAk;
    private String srcSk;
    private String srcCosPath;
    private String srcEndpointSuffix;
    private String srcFileList = "";
    
    public void setSrcFileList(String srcFileList) {
        File localPathFile = new File(srcFileList);
        if (!localPathFile.exists()) {
            throw new IllegalArgumentException("copy file list:[" + srcFileList + "not exist!");
        }
        this.srcFileList = SystemUtils.formatLocalPath(srcFileList);
    }

    public String getSrcFileList() {
        return srcFileList;
    }
    
    public String getSrcBucket() {
        return srcBucket;
    }

    public void setSrcBucket(String srcBucket) {
        srcBucket = srcBucket.trim();
        String parrtern = ".*-[0-9]{3,}$";
        if (!Pattern.matches(parrtern, srcBucket)) {
            throw new IllegalArgumentException(
                    "SrcBucketName must contain appid. example: test-1250001000");
        }
        this.srcBucket = srcBucket;
    }

    public String getSrcRegion() {
        return srcRegion;
    }

    public void setSrcRegion(String srcRegion) {
        if (srcRegion.trim().isEmpty()) {
            throw new IllegalArgumentException("srcRegion value is missing");
        }
        this.srcRegion = srcRegion.trim();
    }

    public String getSrcAk() {
        return srcAk;
    }

    public void setSrcAk(String srcAk) {
        if (srcAk.trim().isEmpty()) {
            throw new IllegalArgumentException("srcSecretId value is missing");
        }
        this.srcAk = srcAk.trim();
    }

    public String getSrcSk() {
        return srcSk;
    }

    public void setSrcSk(String srcSk) {
        if (srcSk.trim().isEmpty()) {
            throw new IllegalArgumentException("srcSecretKey value is missing");
        }
        this.srcSk = srcSk.trim();
    }

    public String getSrcCosPath() {
        return srcCosPath;
    }

    public void setSrcCosPath(String srcCosPath) {
        if (!srcCosPath.startsWith("/")) {
            throw new IllegalArgumentException("srcCosPath must start with /");
        }
        this.srcCosPath = PathUtils.formatCosFolderPath(srcCosPath);
    }

    public String getSrcEndpointSuffix() {
        return srcEndpointSuffix;
    }

    public void setSrcEndpointSuffix(String srcEndpointSuffix) {
        this.srcEndpointSuffix = srcEndpointSuffix;
    }
}
