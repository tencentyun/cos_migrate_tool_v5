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
    private static  String configFilePath = "conf/config.ini";

    private static final String MIGRATE_TYPE_SECTION_NAME = "migrateType";
    private static final String MIGRATE_TYPE = "type";

    private static final String COMMON_SECTION_NAME = "common";
    private static final String COMMON_REGION = "region";
    private static final String COMMON_BUCKETNAME = "bucketName";
    private static final String COMMON_AK = "secretId";
    private static final String COMMON_SK = "secretKey";
    private static final String COMMON_TOKEN = "token";
    private static final String COMMON_COSPATH = "cosPath";
    private static final String COMMON_DB_COSPATH = "dbCosPath";
    private static final String COMMON_HTTPS = "https";
    private static final String COMMON_SHORT_CONNECTION = "shortConnection";
    private static final String COMMON_TMP = "tmpFolder";
    private static final String COMMON_SMALL_FILE_ThRESHOLD = "smallFileThreshold";
    private static final String COMMON_STORAGE_CLASS = "storageClass";
    private static final String COMMON_SMALL_FILE_EXECUTOR_NUM = "smallFileExecutorNum";
    private static final String COMMON_BIG_FILE_EXECUTOR_NUM = "bigFileExecutorNum";
    private static final String COMMON_BIG_FILE_UPLOAD_PART_SIZE = "bigFileUploadPartSize";
    private static final String COMMON_ENTIRE_FILE_MD5_ATTACHED = "entireFileMd5Attached";
    private static final String COMMON_DAEMON_MODE = "daemonMode";
    private static final String COMMON_DAEMON_MODE_INTERVAL = "daemonModeInterVal";
    private static final String COMMON_EXECUTE_TIME_WINDOW = "executeTimeWindow";
    private static final String COMMON_PROXY_HOST = "proxyHost";
    private static final String COMMON_PROXY_PORT = "proxyPort";
    private static final String COMMOM_ENCRYPTION_TYPE = "encryptionType";
    private static final String COMMON_THREAD_NUM = "threadNum";
    private static final String COMMON_BATCH_TASK_PATH = "batchTaskPath";
    private static final String COMMON_REAL_TIME_COMPARE = "realTimeCompare";
    private static final String COMMON_OUTPUT_FINISHED_FILE = "outputFinishedFileFolder";
    private static final String COMMON_RESUME = "resume";
    private static final String COMMON_SKIP_SAME_PATH = "skipSamePath";
    private static final String COMMON_THREAD_TRAFFIC_LIMIT = "threadTrafficLimit";
    private static final String COMMON_CLIENT_ENCRYPTION = "clientEncryption";
    private static final String COMMON_ENCRYPTION_ALGO = "encryptionAlgo";
    private static final String COMMON_KEYPATH = "keyPath";
    private static final String COMMON_ENCRYPTIV = "encryptIV";
    private static final String COMMON_CHECK = "check";
    private static final String COMMON_ROCKSDB_MAX_OPEN_FILE = "rocksMaxOpenFile";
    private static final String COMMON_REQUEST_TIMEOUT_ENABLE = "requestTimeoutEnable";
    private static final String COMMON_REQUEST_TIMEOUT_MS = "requestTimeoutMs";
    private static final String COMMON_REQUEST_TRY_COUNT = "requestTryCount";

    private static final String LOCAL_SECTION_NAME = "migrateLocal";
    private static final String LOCAL_LOCALPATH = "localPath";
    private static final String LOCAL_EXECLUDE = "exeludes";
    private static final String IGNORE_MODIFIED_TIME_LESS_THAN =
            "ignoreModifiedTimeLessThanSeconds";
    private static final String IGNORE_SUFFIX = "ignoreSuffix";
    private static final String INCLUDE_SUFFIX = "includeSuffix";
    private static final String IGNORE_EMPTY_FILE = "ignoreEmptyFile";
    private static final String FILE_LIST_MODE = "fileListMode";
    private static final String FILE_LIST_PATH = "fileListPath";
    private static final String CHECK_LOCAL_RECORD = "checkLocalRecord";

    private static final String LOCAL_TO_COSN_FS_SECTION_NAME = "migrateLocalToCosnFs";

    private static final String ALI_SECTION_NAME = "migrateAli";
    private static final String AWS_SECTION_NAME = "migrateAws";
    private static final String QINIU_SECTION_NAME = "migrateQiniu";
    private static final String CSP_SECTION_NAME = "migrateCsp";
    private static final String UPYUN_SECTION_NAME = "migrateUpyun";
    private static final String CSP_MIGRATE_SLASH_END_OBJECT_ACL = "migrateSlashEndObjectAcl";
    private static final String OSS_BUCKET = "bucket";
    private static final String OSS_AK = "accessKeyId";
    private static final String OSS_SK = "accessKeySecret";
    private static final String OSS_PREFIX = "prefix";
    private static final String OSS_DB_PREFIX = "dbPrefix";
    private static final String OSS_END_POINT = "endPoint";
    private static final String OSS_PROXY_HOST = "proxyHost";
    private static final String OSS_PROXY_PORT = "proxyPort";
    private static final String OSS_SRC_HTTPS = "srcHttps";
    private static final String OSS_SRC_SOCKET_TIMEOUT = "srcSocketTimeout";
    private static final String OSS_SRC_CONNECT_TIMEOUT = "srcConnectTimeout";
    private static final String OSS_URL_LIST = "uriList";
    private static final String UPYUN_COMPARE_MD5 = "compareMd5";
    private static final String UPYUN_ASCENDGING_ORDER = "acsendingOrder";

    private static final String QINIU_NEED_SIGN = "needSign";

    private static final String COPY_BUCKET_SECTION_NAME = "migrateBucketCopy";
    private static final String COPY_SRC_REGION = "srcRegion";
    private static final String COPY_SRC_BUCKETNAME = "srcBucketName";
    private static final String COPY_SRC_SECRETID = "srcSecretId";
    private static final String COPY_SRC_SECRETKEY = "srcSecretKey";
    private static final String COPY_SRC_TOKEN = "srcToken";
    private static final String COPY_SRC_COSPATH = "srcCosPath";
    private static final String COPY_SRC_ENDPOINT_SUFFIX = "srcEndPointSuffix";
    private static final String COPY_SRC_FILE_LIST = "srcFileList";
    private static final String COPY_SRC_STORAGE_CLASS = "srcStorageClass";

    private static final String URLLIST_SECTION_NAME = "migrateUrl";
    private static final String URLLIST_PATH = "urllistPath";
    private static final String URLLIST_IS_SKIP_HEAD = "isSkipHead";



    private CommonConfig config;

    private ConfigParser() {}

    public MigrateType getMigrateType() {
        return migrateType;
    }

    public CommonConfig getConfig() {
        return config;
    }

    public static void setConfigFilePath(String configFilePath) {
        ConfigParser.configFilePath = configFilePath;
    }
    
    private Preferences buildConfigPrefs() {
        System.out.println("cos migrate tool config path:" + configFilePath);
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
        } else if (migrateType.equals(MigrateType.MIGRATE_FROM_CSP)) {
            if (!checkMigrateCompetitorConfig(prefs, MigrateType.MIGRATE_FROM_CSP)) {
                return false;
            }
            config = new CopyFromCspConfig();
            if (!initCopyFromCspConfig(prefs, (CopyFromCspConfig) config)) {
                return false;
            }
            
            if (!config.getBatchTaskPath().isEmpty() && !((CopyFromCspConfig)config).getUrlList().isEmpty()) {
                System.out.println("You can't both set batchTaskPath and urlList");
                return false;
            }
        } else if (migrateType.equals(MigrateType.MIGRATE_FROM_UPYUN)) {
            if (!checkMigrateCompetitorConfig(prefs, MigrateType.MIGRATE_FROM_UPYUN)) {
                return false;
            }
            config = new CopyFromUpyunConfig();
            if (!initCopyFromUpyunConfig(prefs, (CopyFromUpyunConfig) config)) {
                return false;
            }
        } else if (migrateType.equals(MigrateType.MIGRATE_FROM_LOCAL_TO_COSN_FS)){
            if (!checkMigrateLocalToCosnFsConfig(prefs)) {
                return false;
            }
            config = new CopyFromLocalToCosnConfig();
            if (!initCopyFromLocalToCosnConfig(prefs, (CopyFromLocalToCosnConfig) config)) {
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
            System.err.println(errMsg + "  || " + e.toString());
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

    private boolean checkMigrateLocalToCosnFsConfig(Preferences prefs) {
        if (!isKeyExist(prefs, LOCAL_TO_COSN_FS_SECTION_NAME, LOCAL_LOCALPATH)) {
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
        
        if (migrateType != MigrateType.MIGRATE_FROM_UPYUN && !isKeyExist(prefs, sectionName, OSS_END_POINT)) {
            return false;
        }
        return true;
    }

    private boolean initCommonConfig(Preferences prefs, CommonConfig commonConfig) {

        try {
            String batchTaskPath =
                    getConfigValue(prefs, COMMON_SECTION_NAME, COMMON_BATCH_TASK_PATH);
            
            if (batchTaskPath != null) {
                commonConfig.setBatchTaskPath(batchTaskPath);
            }
            
            if ((batchTaskPath == null) || batchTaskPath.isEmpty()) {
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

                String token = getConfigValue(prefs, COMMON_SECTION_NAME, COMMON_TOKEN);
                if (token != null) {
                    commonConfig.setToken(token);
                }
            }
            
            String cosPathConfig = getConfigValue(prefs, COMMON_SECTION_NAME, COMMON_COSPATH);
            assert (cosPathConfig != null);
            commonConfig.setCosPath(cosPathConfig);

            String dbCosPathConfig = getConfigValue(prefs, COMMON_SECTION_NAME, COMMON_DB_COSPATH);
            if (dbCosPathConfig != null) {
                commonConfig.setDbCosPath(dbCosPathConfig);
            }

            String realTimeCompare = getConfigValue(prefs, COMMON_SECTION_NAME, COMMON_REAL_TIME_COMPARE);
            if ((realTimeCompare != null) && !realTimeCompare.isEmpty()) {
                commonConfig.setRealTimeCompare(realTimeCompare);
            }
            
            String enableHttpsStr = getConfigValue(prefs, COMMON_SECTION_NAME, COMMON_HTTPS);
            assert (enableHttpsStr != null);
            commonConfig.setEnableHttps(enableHttpsStr);

            String shortConnectionStr = getConfigValue(prefs, COMMON_SECTION_NAME, COMMON_SHORT_CONNECTION);
            if (shortConnectionStr != null && !shortConnectionStr.trim().isEmpty()) {
                commonConfig.setShortConnection(shortConnectionStr);
            }

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

            String bigFileUploadPartSizeStr =
                    getConfigValue(prefs, COMMON_SECTION_NAME, COMMON_BIG_FILE_UPLOAD_PART_SIZE);
            if(bigFileUploadPartSizeStr != null) {
                commonConfig.setBigFileUploadPartSize(Long.parseLong(bigFileUploadPartSizeStr));
            }

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

            String proxyHost = getConfigValue(prefs, COMMON_SECTION_NAME, COMMON_PROXY_HOST);
            if (proxyHost != null && !proxyHost.trim().isEmpty()) {
                commonConfig.setProxyHost(proxyHost);
            }

            String encryptionType =
                    getConfigValue(prefs, COMMON_SECTION_NAME, COMMOM_ENCRYPTION_TYPE);
            if (encryptionType != null && !encryptionType.trim().isEmpty()) {
                commonConfig.setEncryptionType(encryptionType);
            }

            int port = -1;
            String portStr = getConfigValue(prefs, COMMON_SECTION_NAME, COMMON_PROXY_PORT);

            if (portStr != null && !portStr.isEmpty()) {
                port = Integer.valueOf(portStr);
                if (port > 0) {
                    commonConfig.setProxyPort(port);
                } else {
                    throw new Exception("invalid cos proxy port");
                }
            }

            String taskExecutorNumberStr =
                    getConfigValue(prefs, COMMON_SECTION_NAME, COMMON_THREAD_NUM);
            if (taskExecutorNumberStr != null && !taskExecutorNumberStr.isEmpty()) {
                commonConfig.setTaskExecutorNumberStr(taskExecutorNumberStr);
            }
            
            
            String finishedFileFolder = getConfigValue(prefs, COMMON_SECTION_NAME, COMMON_OUTPUT_FINISHED_FILE);
            if (finishedFileFolder!=null && !finishedFileFolder.isEmpty()) {
                commonConfig.setOutputFinishedFilePath(finishedFileFolder);
            }

            String resume = getConfigValue(prefs, COMMON_SECTION_NAME, COMMON_RESUME);
            if (resume!=null && !resume.isEmpty()) {
                commonConfig.setResume(resume);
            }
            
            String skipSamePath = getConfigValue(prefs, COMMON_SECTION_NAME, COMMON_SKIP_SAME_PATH);
            if (skipSamePath != null && !skipSamePath.isEmpty()) {
                commonConfig.setSkipSamePath(skipSamePath);
            }

            String threadTrafficLimit = getConfigValue(prefs, COMMON_SECTION_NAME, COMMON_THREAD_TRAFFIC_LIMIT);
            if (threadTrafficLimit != null && !threadTrafficLimit.isEmpty()) {
                commonConfig.setThreadTrafficLimit(threadTrafficLimit);
            }

            String clientEncryption = getConfigValue(prefs, COMMON_SECTION_NAME, COMMON_CLIENT_ENCRYPTION);
            if (clientEncryption != null && clientEncryption.compareToIgnoreCase("on") == 0) {
                commonConfig.setClientEncrypt(true);
            }

            String encryptionAlgo = getConfigValue(prefs, COMMON_SECTION_NAME, COMMON_ENCRYPTION_ALGO);
            if (encryptionAlgo != null && !encryptionAlgo.isEmpty()) {
                commonConfig.setEncryptionAlgo(encryptionAlgo);
            }

            String keyPath = getConfigValue(prefs, COMMON_SECTION_NAME, COMMON_KEYPATH);
            if (keyPath != null && !keyPath.isEmpty()) {
                commonConfig.setKeyPath(keyPath);
            }

            String encryptIV = getConfigValue(prefs, COMMON_SECTION_NAME, COMMON_ENCRYPTIV);
            if (encryptIV != null && !encryptIV.isEmpty()) {
                commonConfig.setEncrytIV(encryptIV);
            }

            String check = getConfigValue(prefs, COMMON_SECTION_NAME, COMMON_CHECK);
            if (check != null && check.compareToIgnoreCase("on") == 0) {
                commonConfig.setCheck(true);
            }

            String rocksDBMaxOpenFile = getConfigValue(prefs, COMMON_SECTION_NAME, COMMON_ROCKSDB_MAX_OPEN_FILE);
            if(rocksDBMaxOpenFile != null && !rocksDBMaxOpenFile.isEmpty()) {
                commonConfig.setRocksDBMaxOpenFile(Integer.parseInt(rocksDBMaxOpenFile));
            }

            String requestTimeoutEnable = getConfigValue(prefs, COMMON_SECTION_NAME, COMMON_REQUEST_TIMEOUT_ENABLE);
            if(requestTimeoutEnable != null && !requestTimeoutEnable.isEmpty()) {
                commonConfig.setRequestTimeoutEnable(requestTimeoutEnable);
            }

            String requestTimeoutMs = getConfigValue(prefs, COMMON_SECTION_NAME, COMMON_REQUEST_TIMEOUT_MS);
            if(requestTimeoutMs != null && !requestTimeoutMs.isEmpty()) {
                commonConfig.setRequestTimeoutMS(requestTimeoutMs);
            }

            String requestTryCount = getConfigValue(prefs, COMMON_SECTION_NAME, COMMON_REQUEST_TRY_COUNT);
            if(requestTryCount != null && !requestTryCount.isEmpty()) {
                commonConfig.setRequestTryCount(requestTryCount);
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
            } else {
                 excludes = getConfigValue(prefs, LOCAL_SECTION_NAME, "excludes");
                 if (excludes != null && !excludes.trim().isEmpty()) {  
                     copyLocalConfig.setExcludes(excludes);
                 }
            }

            String ignoreModifiedTimeLessThanStr =
                    getConfigValue(prefs, LOCAL_SECTION_NAME, IGNORE_MODIFIED_TIME_LESS_THAN);
            if (ignoreModifiedTimeLessThanStr != null
                    && !ignoreModifiedTimeLessThanStr.trim().isEmpty()) {
                copyLocalConfig.setIgnoreModifiedTimeLessThan(ignoreModifiedTimeLessThanStr);
            }
            
            String ignoreSuffix = getConfigValue(prefs, LOCAL_SECTION_NAME, IGNORE_SUFFIX);
            if (ignoreSuffix != null && !ignoreSuffix.trim().isEmpty()) {
                copyLocalConfig.setIgnoreSuffix(ignoreSuffix);
            }

            String includeSuffix = getConfigValue(prefs, LOCAL_SECTION_NAME, INCLUDE_SUFFIX);
            if (includeSuffix != null && !includeSuffix.trim().isEmpty()) {
                copyLocalConfig.setIncludeSuffix(includeSuffix);
            }
            
            String ignoreEmptyFile =  getConfigValue(prefs, LOCAL_SECTION_NAME, IGNORE_EMPTY_FILE);
            if (ignoreEmptyFile != null && (ignoreEmptyFile.compareToIgnoreCase("on") == 0)) {
                copyLocalConfig.setIgnoreEmptyFile(true);
            }
            String fileListMode = getConfigValue(prefs, LOCAL_SECTION_NAME, FILE_LIST_MODE);
            if (fileListMode != null && (fileListMode.compareToIgnoreCase("on") == 0)) {
                copyLocalConfig.setFileListMode(true);
            }
            String fileListPath = getConfigValue(prefs, LOCAL_SECTION_NAME, FILE_LIST_PATH);
            if (fileListPath != null) {
                copyLocalConfig.setFileListPath(fileListPath);
            }

            String strCheckLocal = getConfigValue(prefs, LOCAL_SECTION_NAME, CHECK_LOCAL_RECORD);
            if (strCheckLocal != null) {
                copyLocalConfig.setCheckLocalRecord(strCheckLocal);
            }
        } catch (Exception e) {
            System.err.println(e.getMessage());
            log.error(e.getMessage());
            return false;
        }
        return true;
    }

    private boolean initCopyFromLocalToCosnConfig(Preferences prefs,
                                            CopyFromLocalToCosnConfig copyLocalConfig) {
        if (!initCommonConfig(prefs, copyLocalConfig)) {
            return false;
        }
        try {

            String localPathConfig = getConfigValue(prefs, LOCAL_TO_COSN_FS_SECTION_NAME, LOCAL_LOCALPATH);
            assert (localPathConfig != null);
            copyLocalConfig.setLocalPath(localPathConfig);

            String excludes = getConfigValue(prefs, LOCAL_TO_COSN_FS_SECTION_NAME, LOCAL_EXECLUDE);
            if (excludes != null && !excludes.trim().isEmpty()) {
                copyLocalConfig.setExcludes(excludes);
            } else {
                excludes = getConfigValue(prefs, LOCAL_TO_COSN_FS_SECTION_NAME, "excludes");
                if (excludes != null && !excludes.trim().isEmpty()) {
                    copyLocalConfig.setExcludes(excludes);
                }
            }

            String ignoreModifiedTimeLessThanStr =
                    getConfigValue(prefs, LOCAL_TO_COSN_FS_SECTION_NAME, IGNORE_MODIFIED_TIME_LESS_THAN);
            if (ignoreModifiedTimeLessThanStr != null
                    && !ignoreModifiedTimeLessThanStr.trim().isEmpty()) {
                copyLocalConfig.setIgnoreModifiedTimeLessThan(ignoreModifiedTimeLessThanStr);
            }

            String ignoreSuffix = getConfigValue(prefs, LOCAL_TO_COSN_FS_SECTION_NAME, IGNORE_SUFFIX);
            if (ignoreSuffix != null && !ignoreSuffix.trim().isEmpty()) {
                copyLocalConfig.setIgnoreSuffix(ignoreSuffix);
            }

            String includeSuffix = getConfigValue(prefs, LOCAL_TO_COSN_FS_SECTION_NAME, INCLUDE_SUFFIX);
            if (includeSuffix != null && !includeSuffix.trim().isEmpty()) {
                copyLocalConfig.setIncludeSuffix(includeSuffix);
            }

            String ignoreEmptyFile =  getConfigValue(prefs, LOCAL_TO_COSN_FS_SECTION_NAME, IGNORE_EMPTY_FILE);
            if (ignoreEmptyFile != null && (ignoreEmptyFile.compareToIgnoreCase("on") == 0)) {
                copyLocalConfig.setIgnoreEmptyFile(true);
            }
            String fileListMode = getConfigValue(prefs, LOCAL_TO_COSN_FS_SECTION_NAME, FILE_LIST_MODE);
            if (fileListMode != null && (fileListMode.compareToIgnoreCase("on") == 0)) {
                copyLocalConfig.setFileListMode(true);
            }
            String fileListPath = getConfigValue(prefs, LOCAL_TO_COSN_FS_SECTION_NAME, FILE_LIST_PATH);
            if (fileListPath != null) {
                copyLocalConfig.setFileListPath(fileListPath);
            }

            String strCheckLocal = getConfigValue(prefs, LOCAL_TO_COSN_FS_SECTION_NAME, CHECK_LOCAL_RECORD);
            if (strCheckLocal != null) {
                copyLocalConfig.setCheckLocalRecord(strCheckLocal);
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

            String isSkipHead = getConfigValue(prefs, URLLIST_SECTION_NAME, URLLIST_IS_SKIP_HEAD);
            if ((isSkipHead != null) && (isSkipHead.compareToIgnoreCase("on") == 0)) {
                copyUrllistConfig.setSkipHead(true);
            }
            
            String ak = getConfigValue(prefs, URLLIST_SECTION_NAME, OSS_AK);
            if (ak != null) {
                copyUrllistConfig.setSrcAccessKeyId(ak);
            }
            
            String sk = getConfigValue(prefs, URLLIST_SECTION_NAME, OSS_SK);
            if (sk != null) {
                copyUrllistConfig.setSrcAccessKeySecret(sk);
            }
            
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

        String needSignStr = getConfigValue(prefs, QINIU_SECTION_NAME, QINIU_NEED_SIGN);
        if (needSignStr == null) {
            copyQiniuConfig.setIsNeedSign(true);
            return true;
        }

        needSignStr = needSignStr.trim();
        if (needSignStr.compareToIgnoreCase("off") == 0) {
            copyQiniuConfig.setIsNeedSign(false);
        } else if (needSignStr.compareToIgnoreCase("on") == 0) {
            copyQiniuConfig.setIsNeedSign(true);
        } else {
            String errMsg = "qiniu section needSign invalid,need to be \"on\" or \"off\".\n";
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
    
    private boolean initCopyFromUpyunConfig(Preferences prefs, CopyFromUpyunConfig copyUpyunConfig) {
        if (!initCommonConfig(prefs, copyUpyunConfig)) {
            return false;
        }

        if (!initCopyFromCompetitorConfig(prefs, copyUpyunConfig)) {
            return false;
        }
        
        String isAscendingOrder = getConfigValue(prefs, UPYUN_SECTION_NAME, UPYUN_ASCENDGING_ORDER);
        if (isAscendingOrder != null && !isAscendingOrder.isEmpty()) {
            copyUpyunConfig.setAscendingOrder(isAscendingOrder);
        }
        
        String compareMd5 = getConfigValue(prefs, UPYUN_SECTION_NAME, UPYUN_COMPARE_MD5);
        if (compareMd5 != null) {
            if (compareMd5.compareToIgnoreCase("off") == 0) {
                copyUpyunConfig.setCompareMd5(false);
            } else if (compareMd5.compareToIgnoreCase("on") == 0) {
                copyUpyunConfig.setCompareMd5(true);
            } else {
                String errMsg = "upyun section compareMd5 invalid,need to be \"on\" or \"off\".\n";
                System.err.println(errMsg);
                log.error(errMsg);
                return false;
            }
        }
        
        return true;
    }

    private boolean initCopyFromCspConfig(Preferences prefs, CopyFromCspConfig copyCspConfig) {
        if (!initCommonConfig(prefs, copyCspConfig)) {
            return false;
        }

        if (!initCopyFromCompetitorConfig(prefs, copyCspConfig)) {
            return false;
        }
        String migrateSlashEndObjectAcl = getConfigValue(prefs, CSP_SECTION_NAME, CSP_MIGRATE_SLASH_END_OBJECT_ACL);
        if (migrateSlashEndObjectAcl == null || migrateSlashEndObjectAcl.trim().compareToIgnoreCase("true") == 0) {
            copyCspConfig.setMigrateSlashEndObjectAcl(true);
        } else if (migrateSlashEndObjectAcl.trim().compareToIgnoreCase("false") == 0) {
            copyCspConfig.setMigrateSlashEndObjectAcl(false);
        } else {
            String errMsg = "csp section migrateSlashEndObjectAcl invalid,need to be \"true\" or \"false\".\n";
            System.err.println(errMsg);
            log.error(errMsg);
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
            } else if (this.migrateType == MigrateType.MIGRATE_FROM_CSP) {
                sectionName = CSP_SECTION_NAME;
            } else if (this.migrateType == MigrateType.MIGRATE_FROM_UPYUN) {
                sectionName = UPYUN_SECTION_NAME;
            } else {
                log.error("unknow migrate type %s", migrateType.toString());
                return false;
            }

            String prefixConfig = getConfigValue(prefs, sectionName, OSS_PREFIX);
            if (prefixConfig != null) {
                copyOssConfig.setSrcPrefix(prefixConfig);
            }

            String dbPrefixConfig = getConfigValue(prefs, sectionName, OSS_DB_PREFIX);
            if (dbPrefixConfig != null) {
                copyOssConfig.setDbPrefix(dbPrefixConfig);
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

            if ((portStr != null) && !portStr.isEmpty()) {
                port = Integer.valueOf(portStr);
                if (port > 0) {
                    copyOssConfig.setSrcProxyPort(port);
                } else {
                    throw new Exception("invalid proxy port");
                }
            }

            String urlList = getConfigValue(prefs, sectionName, OSS_URL_LIST);
            if ((urlList != null) && !urlList.isEmpty()) {
                copyOssConfig.setUrlList(urlList);
            }
            String enableSrcHttpsStr = getConfigValue(prefs, sectionName, OSS_SRC_HTTPS);
            if(enableSrcHttpsStr != null && !enableSrcHttpsStr.isEmpty()) {
                copyOssConfig.setEnableSrcHttps(enableSrcHttpsStr);
            }
            String srcSocketTimeout = getConfigValue(prefs, sectionName, OSS_SRC_SOCKET_TIMEOUT);
            if (srcSocketTimeout != null && !srcSocketTimeout.isEmpty()) {
                copyOssConfig.setSrcSocketTimeout(Integer.valueOf(srcSocketTimeout));
            }
            String srcConnectTimeout = getConfigValue(prefs, sectionName, OSS_SRC_CONNECT_TIMEOUT);
            if (srcConnectTimeout != null && !srcConnectTimeout.isEmpty()) {
                copyOssConfig.setSrcConnectTimeout(Integer.valueOf(srcConnectTimeout));
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

            String token = getConfigValue(prefs, COPY_BUCKET_SECTION_NAME, COPY_SRC_TOKEN);
            if (token != null && !token.trim().isEmpty()) {
                copyBucketConfig.setSrcToken(token);
            }
            
            String srcCosPath = getConfigValue(prefs, COPY_BUCKET_SECTION_NAME, COPY_SRC_COSPATH);
            assert (srcCosPath != null);
            copyBucketConfig.setSrcCosPath(srcCosPath);

            String srcEndpointSuffix =
                    getConfigValue(prefs, COPY_BUCKET_SECTION_NAME, COPY_SRC_ENDPOINT_SUFFIX);
            if (srcEndpointSuffix != null && !srcEndpointSuffix.trim().isEmpty()) {
                copyBucketConfig.setSrcEndpointSuffix(srcEndpointSuffix);
            }

            String fileList = getConfigValue(prefs, COPY_BUCKET_SECTION_NAME, COPY_SRC_FILE_LIST);
            if (fileList != null && !fileList.isEmpty()) {
                copyBucketConfig.setSrcFileList(fileList);
            }

            String srcStorageClass = getConfigValue(prefs, COPY_BUCKET_SECTION_NAME, COPY_SRC_STORAGE_CLASS);
            if (srcStorageClass != null && !srcStorageClass.isEmpty()) {
                copyBucketConfig.setSrcStorageClass(srcStorageClass);
            }

        } catch (Exception e) {
            System.err.println(e.getMessage());
            log.error(e.getMessage());
            return false;
        }
        return true;
    }
}
