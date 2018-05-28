package com.qcloud.cos_migrate_tool.utils;

import java.io.File;

import org.joda.time.DateTime;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

public class SystemUtils {
    private static final Logger log = LoggerFactory.getLogger(SystemUtils.class);
    public static boolean isWindowsSystem() {
        String osSystemName = System.getProperty("os.name").toLowerCase();
        return osSystemName.startsWith("win");
    }
    
    public static String getCurrentDateTime() {
        DateTime dateTime = new DateTime();
        return dateTime.toString("yyyy-MM-dd HH:mm:ss");
    }
    
    public static String formatLocalPath(String localPath) throws IllegalArgumentException{
        File localFile = new File(localPath);
        if (!localFile.exists()) {
            String errMsg = "localpath " + localPath + " not exist! maybe the path contail illegal utf-8 letter ";
            log.error(errMsg);
            throw new IllegalArgumentException(errMsg);
        }
        String absolutePath = localFile.getAbsolutePath();
        if (SystemUtils.isWindowsSystem()) {
            absolutePath = absolutePath.replace('\\', '/');
        }
        if (localFile.isDirectory()) {
            absolutePath += "/";
        }
        return absolutePath;
    }
}
