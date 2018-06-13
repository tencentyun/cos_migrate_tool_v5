package com.qcloud.cos_migrate_tool.config;

import java.io.File;
import java.io.IOException;
import java.util.prefs.BackingStoreException;
import java.util.prefs.Preferences;

import org.ini4j.Ini;
import org.ini4j.IniPreferences;
import org.ini4j.InvalidFileFormatException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConfigParser {
    public static final ConfigParser instance = new ConfigParser();

    private static final Logger log = LoggerFactory.getLogger(ConfigParser.class);

    private MigrateType migrateType = null;

    private Ini ini;
    private static final String configFilePath = "conf/config.ini";

    private static final String MIGRATE_TYPE_SECTION_NAME = "migrateType";
    private static final String MIGRATE_TYPE = "type";

    private static final String COMMON_SECTION_NAME = "common";
    private static final String COMMON_REGION = "region";
    private static final String COMMON_ENDPOINT_SUFFIX = "endPointSuffix";
    private static final String COMMON_BUCKETNAME = "bucketName";
    private static final String COMMON_AK = "secretId";
    private static final String COMMON_SK = "secretKey";
    private static final String COMMON_COSPATH = "cosPath";
    private static final String COMMON_HTTPS = "https";
    private static final String COMMON_TMP = "tmpFolder";
    private static final String COMMON_SMALL_FILE_ThRESHOLD = "smallFileThreshold";
    private static final String COMMON_STORAGE_CLASS = "storageClass";
    private static final String COMMON_SMALL_FILE_EXECUTOR_NUM = "smallFileExecutorNum";
    private static final String COMMON_BIG_FILE_EXECUTOR_NUM = "bigFileExecutorNum";
    private static final String COMMON_ENTIRE_FILE_MD5_ATTACHED = "entireFileMd5Attached";
    private static final String COMMON_DAEMON_MODE = "daemonMode";
    private static final String COMMON_DAEMON_MODE_INTERVAL = "daemonModeInterVal";
    private static final String COMMON_EXECUTE_TIME_WINDOW = "executeTimeWindow";

    private static final String LOCAL_SECTION_NAME = "migrateLocal";
    private static final String LOCAL_LOCALPATH = "localPath";
    private static final String LOCAL_EXECLUDE = "exeludes";
    private static final String IGNORE_MODIFIED_TIME_LESS_THAN = "ignoreModifiedTimeLessThanSeconds";

    private static final String ALI_SECTION_NAME = "migrateAli";
    private static final String AWS_SECTION_NAME = "migrateAws";
    private static final String QINIU_SECTION_NAME = "migrateQiniu";
    private static final String OSS_BUCKET = "bucket";
    private static final String OSS_AK = "accessKeyId";
    private static final String OSS_SK = "accessKeySecret";
    private static final String OSS_PREFIX = "prefix";
    private static final String OSS_END_POINT = "endPoint";
    private static final String OSS_PROXY_HOST = "proxyHost";
    private static final String OSS_PROXY_PORT = "proxyPort";
    
    private static final String QINIU_NEED_SIGN = "needSign";

    private static final String COPY_BUCKET_SECTION_NAME = "migrateBucketCopy";
    private static final String COPY_SRC_REGION = "srcRegion";
    private static final String COPY_SRC_BUCKETNAME = "srcBucketName";
    private static final String COPY_SRC_SECRETID = "srcSecretId";
    private static final String COPY_SRC_SECRETKEY = "srcSecretKey";
    private static final String COPY_SRC_COSPATH = "srcCosPath";
    private static final String COPY_SRC_ENDPOINT_SUFFIX = "srcEndPointSuffix";

    private static final String URLLIST_SECTION_NAME = "migrateUrl";
    private static final String URLLIST_PATH = "urllistPath";

    private CommonConfig config;

    private ConfigParser() {}

    public MigrateType getMigrateType() {
        return migrateType;
    }

    public CommonConfig getConfig() {
        return config;
    }

    private Preferences buildConfigPrefs() {
        File configFile = new File(configFilePath);
        if (!configFile.exists()) {
            String errMsg = String.format("config %s not exist or readable!", configFilePath);
            System.err.println(errMsg);
            log.error(errMsg);
            return null;
        }

        try {
            ini = new Ini(configFile);
        } catch (InvalidFileFormatException e) {
            String errMsg = String.format("invalid config format. config %s", configFilePath);
            System.err.println(errMsg);
            log.error(errMsg, e);
            return null;
        } catch (IOException e) {
            String errMsg = String.format("read config failed. config %s", configFilePath);
            System.err.println(errMsg);
            log.error(errMsg, e);
            return null;
        }

        return new IniPreferences(ini);
    }

    private boolean isNodeExist(Preferences preferences, String nodeName) {
        try {
            if (!preferences.nodeExists(MIGRATE_TYPE_SECTION_NAME)) {
                String errMsg = String.format("section %s miss! you mustn't delete it",
                        MIGRATE_TYPE_SECTION_NAME);
                System.err.println(errMsg);
                log.error(errMsg);
                return false;
            }
        } catch (BackingStoreException e) {
            String errMsg = String.format("check section %s failed!", MIGRATE_TYPE_SECTION_NAME);
            System.err.println(errMsg);
            log.error(errMsg, e);
            return false;
        }
        return true;
    }

    private boolean isKeyExist(Preferences preferences, String sectionName, String key) {
        if (!isNodeExist(preferences, sectionName) && !isPropExist(sectionName, key)) {
            return false;
        }
        String configValue = getConfigValue(preferences, sectionName, key);
        if (configValue == null) {
            String errMsg =
                    String.format("config key miss. section: %s, key: %s", sectionName, key);
            System.err.println(errMsg);
            log.error(errMsg);
            return false;
        }
        return true;
    }

    private boolean isPropExist(String sectionName, String key) {
        String cmdKey = sectionName + "." + key;
        return System.getProperty(cmdKey) != null;
    }

    private String getConfigValue(Preferences preferences, String sectionName, String key) {
        String configFileValue = preferences.node(sectionName).get(key, null);
        String configCmdValue = System.getProperty(sectionName + "." + key);
        if (configCmdValue != null) {
            return configCmdValue.trim();
        }
        if (configFileValue != null) {
            return configFileValue.trim();
        }
        return null;
    }

    public boolean parse() {

        Preferences prefs = buildConfigPrefs();
        if (prefs == null) {
            return false;
        }

        if (!checkMigrateTypeConfig(prefs)) {
            return false;
        }

        if (!initMigrateType(prefs)) {
            return false;
        }

        if (!checkCommonConfig(prefs)) {
            return false;
        }

        if (migrateType.equals(MigrateType.MIGRATE_FROM_LOCAL)) {
            if (!checkMigrateLocalConfig(prefs)) {
                return false;
            }
            config = new CopyFromLocalConfig();
            if (!initCopyFromLocalConfig(prefs, (CopyFromLocalConfig) config)) {
                return false;
            }

        } else if (migrateType.equals(MigrateType.MIGRATE_FROM_ALI)) {
            // ali copy
            if (!checkMigrateCompetitorConfig(prefs, MigrateType.MIGRATE_FROM_ALI)) {
                return false;
            }
            config = new CopyFromAliConfig();
            if (!initCopyFromAliConfig(prefs, (CopyFromAliConfig) config)) {
                return false;
            }

        } else if (migrateType.equals(MigrateType.MIGRATE_FROM_AWS)) {
            // aws copy
            if (!checkMigrateCompetitorConfig(prefs, MigrateType.MIGRATE_FROM_AWS)) {
                return false;
            }
            config = new CopyFromAwsConfig();
            if (!initCopyFromAwsConfig(prefs, (CopyFromAwsConfig) config)) {
                return false;
            }

        } else if (migrateType.equals(MigrateType.MIGRATE_FROM_QINIU)) {
            // qiniu copy
            if (!checkMigrateCompetitorConfig(prefs, MigrateType.MIGRATE_FROM_QINIU)) {
                return false;
            }
            config = new CopyFromQiniuConfig();
            if (!initCopyFromQiniuConfig(prefs, (CopyFromQiniuConfig) config)) {
                return false;
            }

        } else if (migrateType.equals(MigrateType.MIGRATE_FROM_COS_BUCKET_COPY)) {
            // bucket copy
            if (!checkMigrateCopyBucketConfig(prefs)) {
                return false;
            }
            config = new CopyBucketConfig();
            if (!initCopyBucketConfig(prefs, (CopyBucketConfig) config)) {
                return false;
            }
        } else if (migrateType.equals(MigrateType.MIGRATE_FROM_URLLIST)) {
            // url copy
            config = new CopyFromUrllistConfig();
            if (!initCopyFromUrllistConfig(prefs, (CopyFromUrllistConfig) config)) {
                return false;
            }
        }
        return true;
    }

    private boolean checkMigrateTypeConfig(Preferences prefs) {
        if (!isKeyExist(prefs, MIGRATE_TYPE_SECTION_NAME, MIGRATE_TYPE)) {
            return false;
        }
        return true;
    }

    private boolean initMigrateType(Preferences prefs) {
        try {
            String migrateTypeStr = getConfigValue(prefs, MIGRATE_TYPE_SECTION_NAME, MIGRATE_TYPE);
            assert (migrateTypeStr != null);
            migrateType = MigrateType.fromValue(migrateTypeStr);
        } catch (IllegalArgumentException e) {
            String errMsg = String.format("invalid config. section:%s, key:%s",
                    MIGRATE_TYPE_SECTION_NAME, MIGRATE_TYPE);
            System.err.println(errMsg);
            log.error(errMsg);
            return false;
        }
        return true;
    }

    private boolean checkCommonConfig(Preferences prefs) {
        if (!isKeyExist(prefs, COMMON_SECTION_NAME, COMMON_REGION)) {
            return false;
        }

        if (!isKeyExist(prefs, COMMON_SECTION_NAME, COMMON_BUCKETNAME)) {
            return false;
        }

        if (!isKeyExist(prefs, COMMON_SECTION_NAME, COMMON_AK)) {
            return false;
        }

        if (!isKeyExist(prefs, COMMON_SECTION_NAME, COMMON_SK)) {
            return false;
        }

        if (!isKeyExist(prefs, COMMON_SECTION_NAME, COMMON_COSPATH)) {
            return false;
        }

        if (!isKeyExist(prefs, COMMON_SECTION_NAME, COMMON_HTTPS)) {
            return false;
        }

        if (!isKeyExist(prefs, COMMON_SECTION_NAME, COMMON_TMP)) {
            return false;
        }

        if (!isKeyExist(prefs, COMMON_SECTION_NAME, COMMON_SMALL_FILE_ThRESHOLD)) {
            return false;
        }

        if (!isKeyExist(prefs, COMMON_SECTION_NAME, COMMON_SMALL_FILE_EXECUTOR_NUM)) {
            return false;
        }

        if (!isKeyExist(prefs, COMMON_SECTION_NAME, COMMON_BIG_FILE_EXECUTOR_NUM)) {
            return false;
        }

        if (!isKeyExist(prefs, COMMON_SECTION_NAME, COMMON_STORAGE_CLASS)) {
            return false;
        }

        if (!isKeyExist(prefs, COMMON_SECTION_NAME, COMMON_ENTIRE_FILE_MD5_ATTACHED)) {
            return false;
        }

        if (!isKeyExist(prefs, COMMON_SECTION_NAME, COMMON_DAEMON_MODE)) {
            return false;
        }

        if (!isKeyExist(prefs, COMMON_SECTION_NAME, COMMON_DAEMON_MODE_INTERVAL)) {
            return false;
        }
        if (!isKeyExist(prefs, COMMON_SECTION_NAME, COMMON_EXECUTE_TIME_WINDOW)) {
            return false;
        }
        return true;
    }

    private boolean checkMigrateLocalConfig(Preferences prefs) {
        if (!isKeyExist(prefs, LOCAL_SECTION_NAME, LOCAL_LOCALPATH)) {
            return false;
        }
        return true;
    }

    private boolean checkMigrateCopyBucketConfig(Preferences prefs) {
        if (!isKeyExist(prefs, COPY_BUCKET_SECTION_NAME, COPY_SRC_REGION)) {
            return false;
        }
        if (!isKeyExist(prefs, COPY_BUCKET_SECTION_NAME, COPY_SRC_BUCKETNAME)) {
            return false;
        }
        if (!isKeyExist(prefs, COPY_BUCKET_SECTION_NAME, COPY_SRC_SECRETID)) {
            return false;
        }
        if (!isKeyExist(prefs, COPY_BUCKET_SECTION_NAME, COPY_SRC_SECRETKEY)) {
            return false;
        }
        if (!isKeyExist(prefs, COPY_BUCKET_SECTION_NAME, COPY_SRC_COSPATH)) {
            return false;
        }
        return true;
    }

    private boolean checkMigrateCompetitorConfig(Preferences prefs, MigrateType migrateType) {
        String sectionName = migrateType.toString();
        if (!isKeyExist(prefs, sectionName, OSS_BUCKET)) {
            return false;
        }
        if (!isKeyExist(prefs, sectionName, OSS_AK)) {
            return false;
        }
        if (!isKeyExist(prefs, sectionName, OSS_SK)) {
            return false;
        }
        if (!isKeyExist(prefs, sectionName, OSS_END_POINT)) {
            return false;
        }
        return true;
    }

    private boolean initCommonConfig(Preferences prefs, CommonConfig commonConfig) {

        try {
            String region = getConfigValue(prefs, COMMON_SECTION_NAME, COMMON_REGION);
            assert (region != null);
            commonConfig.setRegion(region);

            String bucketName = getConfigValue(prefs, COMMON_SECTION_NAME, COMMON_BUCKETNAME);
            assert (bucketName != null);
            commonConfig.setBucketName(bucketName);

            String ak = getConfigValue(prefs, COMMON_SECTION_NAME, COMMON_AK);
            assert (ak != null);
            commonConfig.setAk(ak);

            String sk = getConfigValue(prefs, COMMON_SECTION_NAME, COMMON_SK);
            assert (sk != null);
            commonConfig.setSk(sk);

            String cosPathConfig = getConfigValue(prefs, COMMON_SECTION_NAME, COMMON_COSPATH);
            assert (cosPathConfig != null);
            commonConfig.setCosPath(cosPathConfig);

            String enableHttpsStr = getConfigValue(prefs, COMMON_SECTION_NAME, COMMON_HTTPS);
            assert (enableHttpsStr != null);
            commonConfig.setEnableHttps(enableHttpsStr);

            String tmpFolder = getConfigValue(prefs, COMMON_SECTION_NAME, COMMON_TMP);
            assert (tmpFolder != null);
            commonConfig.setTempFileFolder(tmpFolder);

            String smallFileThreshold =
                    getConfigValue(prefs, COMMON_SECTION_NAME, COMMON_SMALL_FILE_ThRESHOLD);
            assert (smallFileThreshold != null);
            commonConfig.setSmallFileThreshold(smallFileThreshold);

            String smallFileExecutorNumberStr =
                    getConfigValue(prefs, COMMON_SECTION_NAME, COMMON_SMALL_FILE_EXECUTOR_NUM);
            assert (smallFileExecutorNumberStr != null);
            commonConfig.setSmallFileUploadExecutorNum(smallFileExecutorNumberStr);

            String bigFileExecutorNumberStr =
                    getConfigValue(prefs, COMMON_SECTION_NAME, COMMON_BIG_FILE_EXECUTOR_NUM);
            assert (bigFileExecutorNumberStr != null);
            commonConfig.setBigFileUploadExecutorNum(bigFileExecutorNumberStr);

            String storageClassStr =
                    getConfigValue(prefs, COMMON_SECTION_NAME, COMMON_STORAGE_CLASS);
            assert (storageClassStr != null);
            commonConfig.setStorageClass(storageClassStr);


            String entireFileMd5AttachedStr =
                    getConfigValue(prefs, COMMON_SECTION_NAME, COMMON_ENTIRE_FILE_MD5_ATTACHED);
            assert (entireFileMd5AttachedStr != null);
            commonConfig.setEntireFileMd5Attached(entireFileMd5AttachedStr);

            String daemonModeStr = getConfigValue(prefs, COMMON_SECTION_NAME, COMMON_DAEMON_MODE);
            assert (daemonModeStr != null);
            commonConfig.setDaemonMode(daemonModeStr);

            String daemonModeInterValStr =
                    getConfigValue(prefs, COMMON_SECTION_NAME, COMMON_DAEMON_MODE_INTERVAL);
            assert (daemonModeInterValStr != null);
            commonConfig.setDaemonModeInterVal(daemonModeInterValStr);

            String timeWindowStr =
                    getConfigValue(prefs, COMMON_SECTION_NAME, COMMON_EXECUTE_TIME_WINDOW);
            assert (timeWindowStr != null);
            commonConfig.setTimeWindowsStr(timeWindowStr);
            
            String endPointSuffixStr = getConfigValue(prefs, COMMON_SECTION_NAME, COMMON_ENDPOINT_SUFFIX);
            if (endPointSuffixStr != null && !endPointSuffixStr.trim().isEmpty()) {
                commonConfig.setEndpointSuffix(endPointSuffixStr);
            }

        } catch (Exception e) {
            System.err.println(e.getMessage());
            log.error(e.getMessage());
            return false;
        }
        return true;

    }

    private boolean initCopyFromLocalConfig(Preferences prefs,
            CopyFromLocalConfig copyLocalConfig) {
        if (!initCommonConfig(prefs, copyLocalConfig)) {
            return false;
        }
        try {

            String localPathConfig = getConfigValue(prefs, LOCAL_SECTION_NAME, LOCAL_LOCALPATH);
            assert (localPathConfig != null);
            copyLocalConfig.setLocalPath(localPathConfig);

            String excludes = getConfigValue(prefs, LOCAL_SECTION_NAME, LOCAL_EXECLUDE);
            if (excludes != null && !excludes.trim().isEmpty()) {
                copyLocalConfig.setExcludes(excludes);
            }
            
            String ignoreModifiedTimeLessThanStr = getConfigValue(prefs, LOCAL_SECTION_NAME, IGNORE_MODIFIED_TIME_LESS_THAN);
            if (ignoreModifiedTimeLessThanStr != null && !ignoreModifiedTimeLessThanStr.trim().isEmpty()) {
                copyLocalConfig.setIgnoreModifiedTimeLessThan(ignoreModifiedTimeLessThanStr);
            }

        } catch (Exception e) {
            System.err.println(e.getMessage());
            log.error(e.getMessage());
            return false;
        }
        return true;
    }

    private boolean initCopyFromUrllistConfig(Preferences prefs,
            CopyFromUrllistConfig copyUrllistConfig) {
        if (!initCommonConfig(prefs, copyUrllistConfig)) {
            return false;
        }
        try {

            String urllistPath = getConfigValue(prefs, URLLIST_SECTION_NAME, URLLIST_PATH);
            assert (urllistPath != null);
            copyUrllistConfig.setUrllistPath(urllistPath);
        } catch (Exception e) {
            System.err.println(e.getMessage());
            log.error(e.getMessage());
            return false;
        }
        return true;
    }

    private boolean initCopyFromQiniuConfig(Preferences prefs,
            CopyFromQiniuConfig copyQiniuConfig) {
        if (!initCommonConfig(prefs, copyQiniuConfig)) {
            return false;
        }

        if (!initCopyFromCompetitorConfig(prefs, copyQiniuConfig)) {
            return false;
        }

        String needSignStr =
                getConfigValue(prefs, QINIU_SECTION_NAME, QINIU_NEED_SIGN);
        if (needSignStr == null) {
        	copyQiniuConfig.setIsNeedSign(true);
        	return true;
        }
        
        needSignStr = needSignStr.trim();
        if (needSignStr.compareToIgnoreCase("false") == 0) {
        	copyQiniuConfig.setIsNeedSign(false);
        } else if (needSignStr.compareToIgnoreCase("true") == 0) {
        	copyQiniuConfig.setIsNeedSign(true);
        } else {
        	String errMsg = "qiniu section needSign invalid,need to be \"true\" or \"false\".\n";
        	System.err.println(errMsg);
        	log.error(errMsg);
        	return false;
        }
        
        return true;
    }

    private boolean initCopyFromAwsConfig(Preferences prefs, CopyFromAwsConfig copyAwsConfig) {
        if (!initCommonConfig(prefs, copyAwsConfig)) {
            return false;
        }

        if (!initCopyFromCompetitorConfig(prefs, copyAwsConfig)) {
            return false;
        }

        return true;
    }

    private boolean initCopyFromAliConfig(Preferences prefs, CopyFromAliConfig copyAliConfig) {
        if (!initCommonConfig(prefs, copyAliConfig)) {
            return false;
        }

        if (!initCopyFromCompetitorConfig(prefs, copyAliConfig)) {
            return false;
        }

        return true;
    }

    private boolean initCopyFromCompetitorConfig(Preferences prefs,
            CopyFromCompetitorConfig copyOssConfig) {

        try {
            String sectionName;
            if (this.migrateType == MigrateType.MIGRATE_FROM_ALI) {
                sectionName = ALI_SECTION_NAME;
            } else if (this.migrateType == MigrateType.MIGRATE_FROM_AWS) {
                sectionName = AWS_SECTION_NAME;
            } else if (this.migrateType == MigrateType.MIGRATE_FROM_QINIU) {
                sectionName = QINIU_SECTION_NAME;
            } else {
                log.error("unknow migrate type %s", migrateType.toString());
                return false;
            }

            String prefixConfig = getConfigValue(prefs, sectionName, OSS_PREFIX);
            if (prefixConfig != null) {
                copyOssConfig.setSrcPrefix(prefixConfig);
            }

            String bucket = getConfigValue(prefs, sectionName, OSS_BUCKET);
            assert (bucket != null);
            copyOssConfig.setSrcBucket(bucket);

            String accessKeyId = getConfigValue(prefs, sectionName, OSS_AK);
            if (accessKeyId != null) {
                copyOssConfig.setSrcAccessKeyId(accessKeyId);
            }

            String accessKeySecret = getConfigValue(prefs, sectionName, OSS_SK);
            if (accessKeySecret != null) {
                copyOssConfig.setSrcAccessKeySecret(accessKeySecret);
            }

            String endPoint = getConfigValue(prefs, sectionName, OSS_END_POINT);
            // assert (endPoint != null);
            if (endPoint != null) {
                copyOssConfig.setEndpoint(endPoint);
            }

            String proxyHost = getConfigValue(prefs, sectionName, OSS_PROXY_HOST);
            copyOssConfig.setSrcProxyHost(proxyHost);

            int port = -1;
            String portStr = getConfigValue(prefs, sectionName, OSS_PROXY_PORT);

            if (!portStr.isEmpty()) {
                port = Integer.valueOf(portStr);
                if (port > 0) {
                    copyOssConfig.setSrcProxyPort(port);
                } else {
                    throw new Exception("invalid proxy port");
                }
            }

        } catch (Exception e) {
            System.err.println(e.getMessage());
            log.error(e.getMessage());
            return false;
        }
        return true;
    }

    private boolean initCopyBucketConfig(Preferences prefs, CopyBucketConfig copyBucketConfig) {
        if (!initCommonConfig(prefs, copyBucketConfig)) {
            return false;
        }
        try {
            String srcRegion = getConfigValue(prefs, COPY_BUCKET_SECTION_NAME, COPY_SRC_REGION);
            assert (srcRegion != null);
            copyBucketConfig.setSrcRegion(srcRegion);

            String srcBucketName =
                    getConfigValue(prefs, COPY_BUCKET_SECTION_NAME, COPY_SRC_BUCKETNAME);
            assert (srcBucketName != null);
            copyBucketConfig.setSrcBucket(srcBucketName);

            String srcSecretId = getConfigValue(prefs, COPY_BUCKET_SECTION_NAME, COPY_SRC_SECRETID);
            assert (srcSecretId != null);
            copyBucketConfig.setSrcAk(srcSecretId);

            String srcSecretKey =
                    getConfigValue(prefs, COPY_BUCKET_SECTION_NAME, COPY_SRC_SECRETKEY);
            assert (srcSecretKey != null);
            copyBucketConfig.setSrcSk(srcSecretKey);

            String srcCosPath = getConfigValue(prefs, COPY_BUCKET_SECTION_NAME, COPY_SRC_COSPATH);
            assert (srcCosPath != null);
            copyBucketConfig.setSrcCosPath(srcCosPath);
            
            String srcEndpointSuffix = getConfigValue(prefs, COPY_BUCKET_SECTION_NAME, COPY_SRC_ENDPOINT_SUFFIX);
            if (srcEndpointSuffix != null) {
                copyBucketConfig.setSrcEndpointSuffix(srcEndpointSuffix);
            }

        } catch (Exception e) {
            System.err.println(e.getMessage());
            log.error(e.getMessage());
            return false;
        }
        return true;
    }
}
