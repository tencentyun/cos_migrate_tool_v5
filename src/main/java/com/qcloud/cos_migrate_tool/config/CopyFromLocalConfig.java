package com.qcloud.cos_migrate_tool.config;

import java.io.File;
import java.util.HashSet;
import java.util.Set;

import com.qcloud.cos_migrate_tool.utils.SystemUtils;

public class CopyFromLocalConfig extends CommonConfig {
    private String localPath;
    private Set<String> excludes = new HashSet<String>();
    private long ignoreModifiedTimeLessThan = -1;

    public String getLocalPath() {
        return localPath;
    }

    public void setLocalPath(String localPath) throws IllegalArgumentException {
        File localPathFile = new File(localPath);
        if (!localPathFile.exists()) {
            throw new IllegalArgumentException("local path not exist!");
        }
        this.localPath = SystemUtils.formatLocalPath(localPath);
    }

    public void setExcludes(String excludePath) throws IllegalArgumentException {
        excludePath.trim();
        String[] exludePathArray = excludePath.split(";");
        for (String excludePathElement : exludePathArray) {
            File tempFile = new File(excludePathElement);
            if (!tempFile.exists()) {
                throw new IllegalArgumentException("excludePath " + excludePath + " not exist");
            }
            this.excludes.add(SystemUtils.formatLocalPath(tempFile.getAbsolutePath()));
        }
    }

    public boolean isExcludes(String excludePath) {
        return this.excludes.contains(excludePath);
    }
    
    public void setIgnoreModifiedTimeLessThan(String ignoreModifiedTimeLessThanStr) {
        try {
            long number = Long.valueOf(ignoreModifiedTimeLessThanStr);
            if (number <= 0) {
                throw new IllegalArgumentException("legal ignoreModifiedTimeLessThan must be positive");
            }
            this.ignoreModifiedTimeLessThan = number;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("invalid ignoreModifiedTimeLessThan");
        }
    }

    public long getIgnoreModifiedTimeLessThan() {
        return ignoreModifiedTimeLessThan;
    }
}
