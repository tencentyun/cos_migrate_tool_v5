package com.qcloud.cos_migrate_tool.config;

import java.io.File;

import com.qcloud.cos_migrate_tool.utils.SystemUtils;

public class CopyFromUrllistConfig extends CommonConfig {
    private String urllistPath;
  

    public String getUrllistPath() {
        return urllistPath;
    }

    public void setUrllistPath(String urllistPath) throws IllegalArgumentException {
        File localPathFile = new File(urllistPath);
        if (!localPathFile.exists()) {
            throw new IllegalArgumentException("local path not exist!");
        }
        this.urllistPath = SystemUtils.formatLocalPath(urllistPath);
    }

  
}
