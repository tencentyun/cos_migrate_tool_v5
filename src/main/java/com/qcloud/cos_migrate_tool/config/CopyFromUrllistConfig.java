package com.qcloud.cos_migrate_tool.config;

import java.io.File;

import com.qcloud.cos_migrate_tool.utils.SystemUtils;

public class CopyFromUrllistConfig extends CommonConfig {
    private String urllistPath;
    private boolean isSkipHead = false;

    private String srcAccessKeyId;
    private String srcAccessKeySecret;
    
    public boolean IsSkipHead() {
        return isSkipHead;
    }
    
    public void setSkipHead(boolean isSkipHead) {
        this.isSkipHead = isSkipHead;
    }

    public String getUrllistPath() {
        return urllistPath;
    }

    public void setUrllistPath(String urllistPath) throws IllegalArgumentException {
        File localPathFile = new File(urllistPath);
        if (!localPathFile.exists()) {
            throw new IllegalArgumentException("urllistPath not exist!");
        }
        this.urllistPath = SystemUtils.formatLocalPath(urllistPath);
    }

    
    public String getSrcAccessKeyId() {
        return srcAccessKeyId;
    }
    
    public void setSrcAccessKeyId(String accessKeyId) throws IllegalArgumentException {
        if (accessKeyId.isEmpty()) {
            throw new IllegalArgumentException("accessKeyId is empty");
        }
        
        this.srcAccessKeyId = accessKeyId;
    }
    
    public String getSrcAccessKeySecret() {
        return srcAccessKeySecret;
    }
    
    public void setSrcAccessKeySecret(String accessKeySecret) throws IllegalArgumentException {
        if (accessKeySecret.isEmpty()) {
            throw new IllegalArgumentException("accessKeySecret is empty");
        }
        
        this.srcAccessKeySecret = accessKeySecret;
    }
}
