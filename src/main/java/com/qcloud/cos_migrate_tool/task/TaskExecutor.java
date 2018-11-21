package com.qcloud.cos_migrate_tool.task;

import java.io.File;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.qcloud.cos.COSClient;
import com.qcloud.cos.ClientConfig;
import com.qcloud.cos.auth.BasicCOSCredentials;
import com.qcloud.cos.auth.COSCredentials;
import com.qcloud.cos.http.HttpProtocol;
import com.qcloud.cos.region.Region;
import com.qcloud.cos.transfer.TransferManager;
import com.qcloud.cos_migrate_tool.config.CommonConfig;
import com.qcloud.cos_migrate_tool.config.MigrateType;
import com.qcloud.cos_migrate_tool.meta.TaskStatics;
import com.qcloud.cos_migrate_tool.record.RecordDb;
import com.qcloud.cos_migrate_tool.utils.SystemUtils;

public abstract class TaskExecutor {
    private static final Logger log = LoggerFactory.getLogger(TaskExecutor.class);

    protected MigrateType migrateType;
    protected final int smallFileUploadExecutorNum;
    protected final int bigFileUploadExecutorNum;
    protected RecordDb recordDb = new RecordDb();
    protected Semaphore semaphore = new Semaphore(1024); // 控制最大添加到任务队列里的任务数
    protected ExecutorService threadPool;
    protected CommonConfig config;

    protected COSClient cosClient;
    protected TransferManager smallFileTransferManager;
    protected TransferManager bigFileTransferManager;

   
    
    enum RUN_MODE {
        NORMAL, DUMP_REQUESTID, QUERY_REQUESTID;
    }

    public TaskExecutor(MigrateType migrateType, CommonConfig config) {
        this.migrateType = migrateType;
        this.config = config;
        this.threadPool = Executors.newFixedThreadPool(config.getTaskExecutorNumber());
        this.smallFileUploadExecutorNum = config.getSmallFileExecutorNumber();
        this.bigFileUploadExecutorNum = config.getBigFileExecutorNum();

        COSCredentials cred = new BasicCOSCredentials(config.getAk(), config.getSk());
        ClientConfig clientConfig = new ClientConfig(new Region(config.getRegion()));
        if (config.isEnableHttps()) {
            clientConfig.setHttpProtocol(HttpProtocol.https);
        }
        if (config.getEndpointSuffix() != null) {
            clientConfig.setEndPointSuffix(config.getEndpointSuffix());
        }
        clientConfig.setUserAgent("cos-migrate-tool-v1.0");

        if (!config.getProxyHost().isEmpty() && config.getProxyPort() > 0) {
            clientConfig.setHttpProxyIp(config.getProxyHost());
            clientConfig.setHttpProxyPort(config.getProxyPort());
        }
        
        this.cosClient = new COSClient(cred, clientConfig);

        this.smallFileTransferManager = new TransferManager(this.cosClient,
                Executors.newFixedThreadPool(config.getSmallFileExecutorNumber()));
        this.smallFileTransferManager.getConfiguration()
                .setMultipartUploadThreshold(config.getSmallFileThreshold());
        this.bigFileTransferManager = new TransferManager(this.cosClient,
                Executors.newFixedThreadPool(config.getBigFileExecutorNum()));
        this.bigFileTransferManager.getConfiguration()
                .setMultipartUploadThreshold(config.getSmallFileThreshold());
    }

    protected abstract String buildTaskDbComment();

    protected abstract String buildTaskDbFolderPath();

    // 初始化Record db
    protected boolean initRecord() {
        String comment = buildTaskDbComment();
        String dbFolderPath = buildTaskDbFolderPath();
        File dbFolder = new File(dbFolderPath);
        if (!dbFolder.isDirectory()) {
            boolean mkdirRet = dbFolder.mkdirs();
            if (!mkdirRet) {
                log.error("make db folder fail! [db_folder_path: %s]", dbFolderPath);
                return false;
            }
        }
        return recordDb.init(dbFolderPath, comment);
    }

    protected void AddTask(Task task) throws InterruptedException {
        try {
            semaphore.acquire();
            threadPool.submit(task);
        } catch (InterruptedException e) {
            log.error("add task is interrupted", e);
            throw e;
        }
    }

    // 用于产生任务
    public abstract void buildTask();

    private RUN_MODE getRunMode() {
        final String runMode = "RUN_MODE";
        String debugModeValue = System.getenv(runMode);
        if (debugModeValue == null || debugModeValue.equalsIgnoreCase("NORMAL")) {
            return RUN_MODE.NORMAL;
        } else if (debugModeValue.equalsIgnoreCase("QUERY_REQUESTID")) {
            return RUN_MODE.QUERY_REQUESTID;
        } else if (debugModeValue.equalsIgnoreCase("DUMP_REQUESTID")) {
            return RUN_MODE.DUMP_REQUESTID;
        }
        return RUN_MODE.NORMAL;
    }

    private String getDumpRequestIdFilePath() {
        final String dumpRequestIdFile = "DUMP_REQUESTID_FILE";
        String dumpFilePath = System.getenv(dumpRequestIdFile);
        if (dumpFilePath == null) {
            String errMsg = "env " + dumpRequestIdFile + " is null";
            System.err.println(errMsg);
            log.error(errMsg);
        }
        return dumpFilePath;
    }

    private String getQueryKey() {
        final String queryKey = "QUERY_REQUESTID_KEY";
        String queryKeyValue = System.getenv(queryKey);
        if (queryKeyValue == null) {
            String errMsg = "env " + queryKey + " is null";
            System.err.println(errMsg);
            log.error(errMsg);
        }
        return queryKeyValue;
    }

    public void run() {
        if (!initRecord()) {
            String errMsg = "init db error, may be another process with same config is running ";
            log.error(errMsg);
            System.err.println(errMsg);
            return;
        }

        RUN_MODE runMode = getRunMode();
        if (runMode.equals(RUN_MODE.NORMAL)) {
            buildTask();
        } else if (runMode.equals(RUN_MODE.DUMP_REQUESTID)) {
            String dumpFilePath = getDumpRequestIdFilePath();
            if (dumpFilePath != null) {
                recordDb.dumpRequestId(dumpFilePath);
            }
        } else if (runMode.equals(RUN_MODE.QUERY_REQUESTID)) {
            String queryKey = getQueryKey();
            if (queryKey != null) {
                recordDb.queryRequestId(queryKey);
            }
        }
    }

    public void waitTaskOver() {
        this.threadPool.shutdown();
        try {
            this.threadPool.awaitTermination(1000, TimeUnit.DAYS);
            this.recordDb.shutdown();
            this.smallFileTransferManager.shutdownNow();
            this.bigFileTransferManager.shutdownNow();
            this.cosClient.shutdown();
            if (getRunMode().equals(RUN_MODE.NORMAL)) {
                printTaskStaticsInfo();
            }
        } catch (InterruptedException e) {
            log.error("waitTaskOver is interrupted!", e);
            System.err.println("waitTaskOver is interrupted!");
        }
    }

    public void printTaskStaticsInfo() {
        String opStatus = "";
        if (TaskStatics.instance.getListFinished() && TaskStatics.instance.getFailCnt() == 0) {
            opStatus = "ALL_OK";
        } else if (TaskStatics.instance.getSuccessCnt() == 0 && TaskStatics.instance.getUpdateCnt() == 0) {
            opStatus = "ALL_FAIL";
        } else {
            opStatus = "PART_OK";
        }

        String printStr = String.format("\n\nlist finished:%s", TaskStatics.instance.getListFinished());
        System.out.println(printStr);
        log.info(printStr);
        
        printStr = String.format("%s over! op statistics:", migrateType.toString());
        System.out.println(printStr);
        log.info(printStr);
        
        printStr = String.format("%30s : %s", "op_status", opStatus);
        System.out.println(printStr);
        log.info(printStr);
        printStr = String.format("%30s : %d", "migrate_new", TaskStatics.instance.getSuccessCnt());
        System.out.println(printStr);
        log.info(printStr);
        printStr = String.format("%30s : %d", "migrate_update", TaskStatics.instance.getUpdateCnt());
        System.out.println(printStr);
        log.info(printStr);
        printStr = String.format("%30s : %d", "migrate_fail", TaskStatics.instance.getFailCnt());
        System.out.println(printStr);
        log.info(printStr);
        printStr = String.format("%30s : %d", "migrate_skip", TaskStatics.instance.getSkipCnt());
        System.out.println(printStr);
        log.info(printStr);
        printStr = String.format("%30s : %d", "migrate_condition_not_match",
                TaskStatics.instance.getConditionNotMatchCnt());
        System.out.println(printStr);
        log.info(printStr);
        printStr = String.format("%30s : %s", "start_time", TaskStatics.instance.getStartTimeStr());
        System.out.println(printStr);
        log.info(printStr);
        printStr = String.format("%30s : %s", "end_time", SystemUtils.getCurrentDateTime());
        System.out.println(printStr);
        log.info(printStr);
        printStr = String.format("%30s : %d s", "used_time",
                TaskStatics.instance.getUsedTimeSeconds());
        System.out.println(printStr);
        log.info(printStr);
    }

}
