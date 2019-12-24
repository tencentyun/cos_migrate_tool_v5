package com.qcloud.cos_migrate_tool.task;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.util.Date;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.logging.Logger;

import com.UpYun;

import com.qcloud.cos.COSClient;
import com.qcloud.cos.exception.CosClientException;
import com.qcloud.cos.exception.CosServiceException;
import com.qcloud.cos.transfer.TransferManager;
import com.qcloud.cos.utils.UrlEncoderUtils;
import com.qcloud.cos_migrate_tool.config.CopyFromAliConfig;
import com.qcloud.cos_migrate_tool.config.CopyFromUpyunConfig;
import com.qcloud.cos_migrate_tool.config.MigrateType;
import com.qcloud.cos_migrate_tool.meta.TaskStatics;
import com.qcloud.cos_migrate_tool.record.MigrateCompetitorRecordElement;
import com.qcloud.cos_migrate_tool.record.RecordDb;
import com.upyun.UpException;

public class MigrateUpyunTask extends Task {

    private UpYun upyun;
    private String srcKey;
    private long fileSize;
    private String etag = "";
    private Date lastModify;
    private String contentType;

    public MigrateUpyunTask(CopyFromUpyunConfig config, UpYun upyun, String srcKey, long fileSize,
            Date lastModify, String contentType, TransferManager smallFileTransfer, TransferManager bigFileTransfer,
            RecordDb recordDb, Semaphore semaphore) {
        super(semaphore, config, smallFileTransfer, bigFileTransfer, recordDb);
        this.upyun = upyun;
        this.srcKey = srcKey;
        this.fileSize = fileSize;
        this.contentType = contentType;

        this.lastModify = lastModify;
        if (srcKey.startsWith("/")) {
            this.srcKey = srcKey.substring(1);
        }

    }

    private String buildCOSPath() {
        String srcPrefix = ((CopyFromUpyunConfig) config).getSrcPrefix();
        int lastDelimiter = srcPrefix.lastIndexOf("/");
        if (lastDelimiter == 0) {
            lastDelimiter = -1;
        }
        String keyName = srcKey.substring(lastDelimiter + 1);
        String cosPrefix = config.getCosPath();
        if (cosPrefix.endsWith("/")) {
            return cosPrefix + keyName;
        } else {
            return cosPrefix + "/" + keyName;
        }
    }


    @Override
    public void doTask() {

        String cosPath = buildCOSPath();

        this.etag = this.lastModify.toString();
        // System.out.println(this.etag);

        MigrateCompetitorRecordElement upyunRecordElement = new MigrateCompetitorRecordElement(
                MigrateType.MIGRATE_FROM_UPYUN, config.getBucketName(), cosPath, etag, fileSize);

        if (config.getRealTimeCompare()) {
            // head
            try {
                com.qcloud.cos.model.ObjectMetadata dstMeta = this.smallFileTransfer.getCOSClient()
                        .getObjectMetadata(config.getBucketName(), cosPath);

                if (dstMeta.getLastModified().after(lastModify)) {
                    TaskStatics.instance.addSkipCnt();
                    return;
                }
            } catch (CosServiceException e) {
                if (e.getStatusCode() != 404) {
                    log.error("[fail] task_info: {}, exception: {}", upyunRecordElement.buildKey(),
                            e.toString());
                    TaskStatics.instance.addFailCnt();
                    if (e.getStatusCode() == 503) {
                        try {
                            Thread.sleep(100);
                        } catch (InterruptedException e1) {
                            // TODO Auto-generated catch block
                            e1.printStackTrace();
                        }
                    }
                    return;
                }
            } catch (Exception e) {
                log.error("[fail] task_info: {}, exception: {}", upyunRecordElement.buildKey(),
                        e.toString());
                TaskStatics.instance.addFailCnt();
                return;
            }

        } else if (isExist(upyunRecordElement, true)) {
            TaskStatics.instance.addSkipCnt();
            return;
        }


        String localPath = config.getTempFolderPath() + UUID.randomUUID().toString();
        File localFile = new File(localPath);
        try {
            boolean success =
                    upyun.readFile(UrlEncoderUtils.encodeEscapeDelimiter(this.srcKey), localFile);
            if (!success) {
                String errMsg = String.format("[fail] taskInfo: %s, No Exception",
                        upyunRecordElement.buildKey());
                System.err.println(errMsg);
                log.error(errMsg);
                TaskStatics.instance.addFailCnt();
                if (localFile.exists()) {
                    localFile.delete();
                }
            }
        } catch (Exception e) {
            String errMsg = String.format("[fail] taskInfo: %s, Caught an Exception, error msg: %s",
                    upyunRecordElement.buildKey(), e.toString());
            System.err.println(errMsg);
            log.error(errMsg);
            TaskStatics.instance.addFailCnt();
            if (localFile.exists()) {
                localFile.delete();
            }
            return;
        }

        // upload

        if (!localFile.exists()) {
            String errMsg = String.format("[fail] taskInfo: %s. file: %s not exist, srcKey: %s",
                    upyunRecordElement.buildKey(), localPath, srcKey);
            System.err.println(errMsg);
            log.error(errMsg);
            TaskStatics.instance.addFailCnt();
            return;
        }

        if (localFile.length() != this.fileSize) {
            String errMsg = String.format(
                    "[fail] taskInfo: %s, download file size %d not equal meta size %d",
                    upyunRecordElement.buildKey(), localFile.length(), this.fileSize);
            System.err.println(errMsg);
            log.error(errMsg);
            TaskStatics.instance.addFailCnt();
            localFile.delete();
            return;
        }

        com.qcloud.cos.model.ObjectMetadata cosMetadata = new com.qcloud.cos.model.ObjectMetadata();
        cosMetadata.setContentType(this.contentType);

        try {
            String requestId = uploadFile(config.getBucketName(), cosPath, localFile,
                    config.getStorageClass(), config.isEntireFileMd5Attached(), cosMetadata, null);
            saveRecord(upyunRecordElement);
            saveRequestId(cosPath, requestId);
            if (this.query_result == RecordDb.QUERY_RESULT.KEY_NOT_EXIST) {
                TaskStatics.instance.addSuccessCnt();
            } else {
                TaskStatics.instance.addUpdateCnt();
            }
            String printMsg = String.format("[ok] [requestid: %s], task_info: %s",
                    requestId == null ? "NULL" : requestId, upyunRecordElement.buildKey());
            System.out.println(printMsg);
            log.info(printMsg);
        } catch (Exception e) {
            String printMsg = String.format("[fail] task_info: %s", upyunRecordElement.buildKey());
            System.err.println(printMsg);
            log.error("[fail] task_info: {}, exception: {}", upyunRecordElement.buildKey(),
                    e.toString());
            TaskStatics.instance.addFailCnt();
        } finally {
            localFile.delete();
        }

    }
}
