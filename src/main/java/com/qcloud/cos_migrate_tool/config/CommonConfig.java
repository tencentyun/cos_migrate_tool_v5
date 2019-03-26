package com.qcloud.cos_migrate_tool.config;

import java.io.File;
import java.util.regex.Pattern;

import com.qcloud.cos.model.StorageClass;
import com.qcloud.cos_migrate_tool.utils.PathUtils;
import com.qcloud.cos_migrate_tool.utils.SystemUtils;

/**
 * @author chengwu 定义common配置, 比如tempfile, COS的账户信息
 */
public class CommonConfig {

    private String tempFolderPath;
    private String bucketName;
    private String region;
    private String ak;
    private String sk;
    private String cosPath;
    private boolean enableHttps;
    private boolean entireFileMd5Attached;
    private int taskExecutorNumber = 64;
    private StorageClass storageClass = StorageClass.Standard;
    private int smallFileExecutorNumber = 64;
    private int bigFileExecutorNum = 4;
    private long smallFileThreshold = 5 * 1024 * 1024;
    private boolean damonMode = false;
    private long damonInterVal = 60;
    private int timeWindowBegin = 0;
    private int timeWindowEnd = 24;
    private String endpointSuffix = null;
    private String cosProxyHost = "";
    private int cosProxyPort = -1;
    private String encryptionType = "";

    public void setEncryptionType(String encryptionType) {
        if (!encryptionType.equals("sse-cos")) {
            throw new IllegalArgumentException("Not support encryptionType:" + encryptionType);
        }
        
        this.encryptionType = encryptionType.trim();
    }
    
    public String getEncryptionType() {
        return encryptionType;
    }
    
    public String getTempFolderPath() {
        return tempFolderPath;
    }

    public void setTempFileFolder(String tempFolderPath) {
        tempFolderPath = tempFolderPath.trim();
        File tempFolder = new File(tempFolderPath);
        if (!tempFolder.exists()) {
            throw new IllegalArgumentException("tempFolderPath " + tempFolderPath + " not exist!");
        }

        if (tempFolderPath.endsWith("/") || tempFolderPath.endsWith("\\")) {
            this.tempFolderPath = tempFolderPath;
        } else {
            this.tempFolderPath = tempFolderPath + "/";
        }
        this.tempFolderPath = SystemUtils.formatLocalPath(this.tempFolderPath);
    }

    public String getBucketName() {
        return bucketName;
    }

    public void setBucketName(String bucketName) {
        bucketName = bucketName.trim();
        String parrtern = ".*-(125|100|20)[0-9]{3,}$";
        if (!Pattern.matches(parrtern, bucketName)) {
            throw new IllegalArgumentException(
                    "bucketName must contain appid. example: test-1250001000");
        }
        this.bucketName = bucketName;
    }

    public String getRegion() {
        return region;
    }

    public void setRegion(String region) throws IllegalArgumentException {
        if (region.trim().isEmpty()) {
            throw new IllegalArgumentException("region value is missing");
        }
        this.region = region.trim();
    }
    
    public String getEndpointSuffix() {
        return endpointSuffix;
    }

    public void setEndpointSuffix(String endpointSuffix) {
        this.endpointSuffix = endpointSuffix;
    }

    public String getAk() {
        return ak;
    }

    public void setAk(String ak) throws IllegalArgumentException {
        ak = ak.trim();
        if (ak.isEmpty()) {
            throw new IllegalArgumentException("secretId is missing");
        }
        this.ak = ak;
    }

    public String getSk() {
        return sk;
    }

    public void setSk(String sk) {
        sk = sk.trim();
        if (sk.isEmpty()) {
            throw new IllegalArgumentException("secretValue is missing");
        }
        this.sk = sk;
    }

    public String getCosPath() {
        return cosPath;
    }

    public void setCosPath(String cosPath) throws IllegalArgumentException {
        if (!cosPath.startsWith("/")) {
            throw new IllegalArgumentException("cospath must start with /");
        }
        this.cosPath = PathUtils.formatCosFolderPath(cosPath);
    }

    public boolean isEnableHttps() {
        return enableHttps;
    }

    public void setEnableHttps(String enableHttpsStr) throws IllegalArgumentException {
        if (enableHttpsStr.equalsIgnoreCase("on")) {
            this.enableHttps = true;
        } else if (enableHttpsStr.equalsIgnoreCase("off")) {
            this.enableHttps = false;
        } else {
            throw new IllegalArgumentException("invalid https config. only support on/off");
        }
    }

    public void setTaskExecutorNumberStr(String taskExecutorNumberStr)
            throws IllegalArgumentException {
        taskExecutorNumberStr = taskExecutorNumberStr.trim();
        try {
            int number = Integer.valueOf(taskExecutorNumberStr);
            if (number <= 0) {
                throw new IllegalArgumentException("taskExecutorNumber must be greater than 0");
            }
            this.taskExecutorNumber = number;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("invalid taskExecutorNumber");
        }
    }

    public int getTaskExecutorNumber() {
        return taskExecutorNumber;
    }

    public StorageClass getStorageClass() {
        return storageClass;
    }

    public void setStorageClass(String storageClassStr) throws IllegalArgumentException {
        try {
            this.storageClass = StorageClass.valueOf(storageClassStr);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(storageClassStr + " is invalid storage class!");
        }
    }


    public int getSmallFileExecutorNumber() {
        return smallFileExecutorNumber;
    }

    public void setSmallFileUploadExecutorNum(String smallFileExecutorNumStr)
            throws IllegalArgumentException {
        smallFileExecutorNumStr = smallFileExecutorNumStr.trim();
        try {
            int number = Integer.valueOf(smallFileExecutorNumStr);
            if (number <= 0 && number > 1024) {
                throw new IllegalArgumentException("legal smallFileExecutorNum  is [1, 1024] ");
            }
            this.smallFileExecutorNumber = number;
            this.taskExecutorNumber = this.smallFileExecutorNumber + this.bigFileExecutorNum;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("invalid smallFileExecutorNum");
        }
    }

    public int getBigFileExecutorNum() {
        return bigFileExecutorNum;
    }

    public void setBigFileUploadExecutorNum(String bigFileExecutorNumStr) {
        bigFileExecutorNumStr = bigFileExecutorNumStr.trim();
        try {
            int number = Integer.valueOf(bigFileExecutorNumStr);
            if (number <= 0 && number > 64) {
                throw new IllegalArgumentException("legal bigFileExecutorNum is [1, 64] ");
            }
            this.bigFileExecutorNum = number;
            this.taskExecutorNumber = this.smallFileExecutorNumber + this.bigFileExecutorNum;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("invalid bigFileExecutorNum");
        }
    }

    public void setSmallFileThreshold(String smallFileThresholdStr) {
        smallFileThresholdStr = smallFileThresholdStr.trim();
        try {
            long number = Long.valueOf(smallFileThresholdStr);
            final long minSmallFile = 5 * 1024 * 1024; // 最小5MB
            final long maxSmallFile = 5 * 1024 * 1024 * 1024; // 最大5GB
            if (number < minSmallFile && number > maxSmallFile) {
                throw new IllegalArgumentException(String.format(
                        "legal smallFileThreshold is [%d, %d], 5MB ~ 5GB", minSmallFile, maxSmallFile));
            }
            this.smallFileThreshold = number;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("invalid smallFileThreshold");
        }
    }
    
    public long getSmallFileThreshold() {
        return smallFileThreshold;
    }

    public boolean isEntireFileMd5Attached() {
        return entireFileMd5Attached;
    }

    public void setEntireFileMd5Attached(String entireFileMd5AttachedStr) {
        if (entireFileMd5AttachedStr.equalsIgnoreCase("on")) {
            this.entireFileMd5Attached = true;
        } else if (entireFileMd5AttachedStr.equalsIgnoreCase("off")) {
            this.entireFileMd5Attached = false;
        } else {
            throw new IllegalArgumentException(
                    "invalid entireFileMd5Attached config. only support on/off");
        }
    }
    
    public void setDaemonMode(String daemonModeStr) {
        if (daemonModeStr.equalsIgnoreCase("on")) {
            this.damonMode = true;
        } else if (daemonModeStr.equalsIgnoreCase("off")) {
            this.damonMode = false;
        } else {
            throw new IllegalArgumentException(
                    "invalid daemonMode config. only support on/off");
        }
    }
    
    public void setDaemonModeInterVal(String daemonModeStr) {
        daemonModeStr = daemonModeStr.trim();
        try {
            int number = Integer.valueOf(daemonModeStr);
            if (number <= 0) {
                throw new IllegalArgumentException("damonInterVal must be greater than 0");
            }
            this.damonInterVal = number;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("invalid damonInterVal");
        }
    }

    public boolean isDamonMode() {
        return damonMode;
    }

    public long getDamonInterVal() {
        return damonInterVal;
    }
    
    public void setTimeWindowsStr(String timeWindowStr) {
        timeWindowStr = timeWindowStr.trim();
        String[] timeWindowArray = timeWindowStr.split(",");
        if (timeWindowArray.length != 2) {
            throw new IllegalArgumentException("executeTimeWindow is invalid, the legal example 03:30,21:00");
        }
        try {
            String[] timeBeginMemberArray = timeWindowArray[0].split(":");
            if (timeBeginMemberArray.length != 2) {
                throw new IllegalArgumentException("executeTimeWindow is invalid, the legal example 03:30,21:00");
            }
            int hour = Integer.valueOf(timeBeginMemberArray[0]);
            if (hour < 0 || hour >= 24) {
                throw new IllegalArgumentException("executeTimeWindow is invalid, the legal example 03:30,21:00");
            }
            int minute = Integer.valueOf(timeBeginMemberArray[1]);
            if (minute < 0 || minute >= 60) {
                throw new IllegalArgumentException("executeTimeWindow is invalid, the legal example 03:30,21:00");
            }
            this.timeWindowBegin = hour * 60 + minute;
            
            String[] timeEndMemberArray = timeWindowArray[1].split(":");
            if (timeEndMemberArray.length != 2) {
                throw new IllegalArgumentException("executeTimeWindow is invalid, the legal example 03:30,21:00");
            }
            hour = Integer.valueOf(timeEndMemberArray[0]);
            if (hour < 0 || hour > 24) {
                throw new IllegalArgumentException("executeTimeWindow is invalid, the legal example 03:30,21:00");
            }
            minute = Integer.valueOf(timeEndMemberArray[1]);
            if (minute < 0 || minute >= 60) {
                throw new IllegalArgumentException("executeTimeWindow is invalid, the legal example 03:30,21:00");
            }
            this.timeWindowEnd = hour * 60 + minute;
            
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("invalid executeTimeWindow");
        }
    }

    public int getTimeWindowBegin() {
        return timeWindowBegin;
    }

    public int getTimeWindowEnd() {
        return timeWindowEnd;
    }
    
    public void setProxyHost(String host) {
        this.cosProxyHost = host;
    }
    
    public void setProxyPort(int port) {
        this.cosProxyPort = port;
    }
    
    public String getProxyHost() {
        return this.cosProxyHost;
    }
    
    public int getProxyPort() {
        return this.cosProxyPort;
    }
}
