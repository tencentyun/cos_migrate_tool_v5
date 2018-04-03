package com.qcloud.cos_migrate_tool.task;

import java.io.File;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;

import com.qcloud.cos.transfer.TransferManager;
import com.qcloud.cos_migrate_tool.config.CopyFromUrllistConfig;
import com.qcloud.cos_migrate_tool.config.MigrateType;
import com.qcloud.cos_migrate_tool.meta.TaskStatics;
import com.qcloud.cos_migrate_tool.record.MigrateUrllistRecordElement;
import com.qcloud.cos_migrate_tool.record.RecordDb;
import com.qcloud.cos_migrate_tool.utils.Downloader;

public class MigrateUrllistTask extends Task {

    private CopyFromUrllistConfig config;
    private String url;
    private String srcKey;

    public MigrateUrllistTask(CopyFromUrllistConfig config, String url, String srcKey,
            TransferManager smallFileTransfer, TransferManager bigFileTransfer, RecordDb recordDb,
            Semaphore semaphore) {
        super(semaphore, smallFileTransfer, bigFileTransfer, config.getSmallFileThreshold(),
                recordDb);

        this.config = config;
        this.url = url;
        this.srcKey = srcKey;
        if (srcKey.startsWith("/")) {
            this.srcKey = srcKey.substring(1);
        }
    }

    private String buildCOSPath() {
        String cosPrefix = config.getCosPath();
        if (cosPrefix.endsWith("/")) {
            return cosPrefix + srcKey;
        } else {
            return cosPrefix + "/" + srcKey;
        }
    }

    @Override
    public void doTask() {

        String cosPath = buildCOSPath();
        String localPath = config.getTempFolderPath() + ThreadLocalRandom.current().nextLong();
        long fileSize = -1;
        try {
            fileSize = Downloader.instance.headFile(url);
        } catch (Exception e) {
            log.error("head file fail, url: {}, msg:{}", url, e.getMessage());
        }

        MigrateUrllistRecordElement urllistRecordElement = new MigrateUrllistRecordElement(
                MigrateType.MIGRATE_FROM_URLLIST, config.getBucketName(), cosPath, url, fileSize);
        if (isExist(urllistRecordElement)) {
            TaskStatics.instance.addSkipCnt();
            return;
        }

        File localFile = new File(localPath);
        boolean downloadSucc = false;
        try {
            downloadSucc = Downloader.instance.downFile(url, localFile);
        } catch (Exception e) {
            String printMsg =
                    String.format("[fail] task_info: %s", urllistRecordElement.buildKey());
            System.err.println(printMsg);
            TaskStatics.instance.addFailCnt();
            log.error("download fail task_info: %s, [msg:{}]", url, e.getMessage());
            localFile.delete();
            return;
        }

        if (!downloadSucc) {
            String printMsg =
                    String.format("[fail] task_info: %s", urllistRecordElement.buildKey());
            System.err.println(printMsg);
            log.error(printMsg);
            TaskStatics.instance.addFailCnt();
            localFile.deleteOnExit();
            return;
        }

        // upload
        if (!localFile.exists()) {
            String errMsg = String.format("[fail] taskInfo: %s. tmpfile: %s not exist",
                    urllistRecordElement.buildKey(), localPath);
            System.err.println(errMsg);
            log.error(errMsg);
            TaskStatics.instance.addFailCnt();
            return;
        }

        try {
            uploadFile(config.getBucketName(), cosPath, localFile, config.getStorageClass(),
                    config.isEntireFileMd5Attached());
            saveRecord(urllistRecordElement);
            TaskStatics.instance.addSuccessCnt();
            String printMsg = String.format("[ok] task_info: %s", urllistRecordElement.buildKey());
            System.out.println(printMsg);
            log.info(printMsg);
        } catch (Exception e) {
            String printMsg =
                    String.format("[fail] task_info: %s", urllistRecordElement.buildKey());
            System.err.println(printMsg);
            log.error("[fail] task_info: {}, exception: {}", urllistRecordElement.buildKey(),
                    e.toString());
            TaskStatics.instance.addFailCnt();
        } finally {
            localFile.delete();
        }

    }
}
