package com.qcloud.cos_migrate_tool.utils;

import java.io.File;

import org.joda.time.DateTime;

public class SystemUtils {
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
            throw new IllegalArgumentException("localpath " + localPath + " not exist!");
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
