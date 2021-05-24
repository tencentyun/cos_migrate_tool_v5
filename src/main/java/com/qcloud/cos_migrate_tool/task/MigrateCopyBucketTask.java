package com.qcloud.cos_migrate_tool.task;

import java.io.File;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;

import com.qcloud.cos.COSClient;
import com.qcloud.cos.endpoint.SuffixEndpointBuilder;
import com.qcloud.cos.exception.CosServiceException;
import com.qcloud.cos.model.CopyObjectRequest;
import com.qcloud.cos.model.CopyResult;
import com.qcloud.cos.model.GetObjectRequest;
import com.qcloud.cos.model.ObjectMetadata;
import com.qcloud.cos.region.Region;
import com.qcloud.cos.transfer.Copy;
import com.qcloud.cos.transfer.TransferManager;
import com.qcloud.cos_migrate_tool.config.CopyBucketConfig;
import com.qcloud.cos_migrate_tool.config.MigrateType;
import com.qcloud.cos_migrate_tool.meta.TaskStatics;
import com.qcloud.cos_migrate_tool.record.MigrateCopyBucketRecordElement;
import com.qcloud.cos_migrate_tool.record.RecordDb;

public class MigrateCopyBucketTask extends Task {
    private final COSClient srcCOSClient;
    private final String destRegion;
    private final String destBucketName;
    private final String destKey;
    private final String srcRegion;
    private final String srcEndpointSuffx;
    private final String srcBucketName;
    private final String srcKey;
    private  long srcSize;
    private  String srcEtag;

    public MigrateCopyBucketTask(Semaphore semaphore, CopyBucketConfig config,
            TransferManager smallFileTransfer, TransferManager bigFileTransfer, RecordDb recordDb,
            COSClient srcCOSClient, String srcKey, long srcSize, String srcEtag, String destKey) {
        super(semaphore, config, smallFileTransfer, bigFileTransfer, recordDb);
        this.srcCOSClient = srcCOSClient;
        this.destRegion = config.getRegion();
        this.destBucketName = config.getBucketName();
        this.destKey = destKey;
        this.srcRegion = config.getSrcRegion();
        this.srcEndpointSuffx = config.getSrcEndpointSuffix();
        this.srcBucketName = config.getSrcBucket();
        this.srcKey = srcKey;
        this.srcSize = srcSize;
        this.srcEtag = srcEtag;
    }


    private void transferFileForNotAllowedCopyObject(MigrateCopyBucketRecordElement copyElement) {
        String downloadTempPath =
                config.getTempFolderPath() + ThreadLocalRandom.current().nextLong();
        File downloadTempFile = new File(downloadTempPath);
        try {
            ObjectMetadata objectMetadata = srcCOSClient
                    .getObject(new GetObjectRequest(srcBucketName, srcKey), downloadTempFile);
            String requestId = uploadFile(destBucketName, destKey, downloadTempFile,
                    config.getStorageClass(), config.isEntireFileMd5Attached(), objectMetadata, null);
            saveRecord(copyElement);
            saveRequestId(destKey, requestId);
            if (this.query_result == RecordDb.QUERY_RESULT.KEY_NOT_EXIST) {
                TaskStatics.instance.addSuccessCnt();
            } else {
                TaskStatics.instance.addUpdateCnt();
            }
            String printMsg = String.format("[ok] [requestid: %s], task_info: %s",
                    requestId == null ? "NULL" : requestId,
                    copyElement.buildKey());
            System.out.println(printMsg);
            log.info(printMsg);
        } catch (Exception e) {
            String printMsg = String.format("[fail] task_info: %s",
                    copyElement.buildKey());
            System.out.println(printMsg);
            log.error("fail! task_info: [key: {}], [value: {}], exception: {}",
                    copyElement.buildKey(),
                    copyElement.buildValue(), e.toString());
            TaskStatics.instance.addFailCnt();
        }
    }


    @Override
    public void doTask() {
        
        if (srcEtag.isEmpty()) {
            ObjectMetadata objectMetadata = srcCOSClient.getObjectMetadata(srcBucketName, srcKey);
            srcEtag = objectMetadata.getETag();
            this.srcSize = objectMetadata.getContentLength();
        }
 
        MigrateCopyBucketRecordElement migrateCopyBucketRecordElement =
                new MigrateCopyBucketRecordElement(destRegion, destBucketName, destKey, srcRegion,
                        srcBucketName, srcKey, srcSize, srcEtag);
        if (isExist(migrateCopyBucketRecordElement, true)) {
            TaskStatics.instance.addSkipCnt();
            return;
        }

        if (config.skipSamePath()) {
            try {
                if (isExistOnCOS(smallFileTransfer, MigrateType.MIGRATE_FROM_COS_BUCKET_COPY, destBucketName, destKey)) {
                    TaskStatics.instance.addSkipCnt();
                    return;
                }
            } catch (Exception e) {
                String printMsg = String.format("[fail] task_info: %s", migrateCopyBucketRecordElement.buildKey());
                System.err.println(printMsg);
                log.error("[fail] task_info: {}, exception: {}", migrateCopyBucketRecordElement.buildKey(), e.toString());
                TaskStatics.instance.addFailCnt();
                return;
            }
        }

        CopyObjectRequest copyObjectRequest = new CopyObjectRequest(new Region(srcRegion),
                srcBucketName, srcKey, destBucketName, destKey);
        
        if (srcBucketName.equals(destBucketName) && ( destKey.equals("/" + srcKey) || srcKey.equals(destKey)) ) {
            ObjectMetadata newObjectMetadata = new ObjectMetadata();
            newObjectMetadata.addUserMetadata("x-cos-metadata-directive", "Replaced");
            copyObjectRequest.setNewObjectMetadata(newObjectMetadata);
        }
        
        if (srcEndpointSuffx != null && !srcEndpointSuffx.isEmpty()) {
            SuffixEndpointBuilder sourceEndpointBuilder = new SuffixEndpointBuilder(srcEndpointSuffx);
            copyObjectRequest.setSourceEndpointBuilder(sourceEndpointBuilder);
        }
        
        copyObjectRequest.setStorageClass(this.config.getStorageClass());
        
        try {
            Copy copy = smallFileTransfer.copy(copyObjectRequest, srcCOSClient, null);
            CopyResult copyResult = copy.waitForCopyResult();
            String requestId = copyResult.getRequestId();
            saveRecord(migrateCopyBucketRecordElement);
            saveRequestId(destKey, requestId);
            if (this.query_result == RecordDb.QUERY_RESULT.KEY_NOT_EXIST) {
                TaskStatics.instance.addSuccessCnt();
            } else {
                TaskStatics.instance.addUpdateCnt();
            }
            String printMsg = String.format("[ok] [requestid: %s], task_info: %s",
                    requestId == null ? "NULL" : requestId,
                    migrateCopyBucketRecordElement.buildKey());
            System.out.println(printMsg);
            log.info(printMsg);
        } catch (Exception e) {
            if (e instanceof CosServiceException) {
                if (((CosServiceException) e).getStatusCode() == 405) {
                    log.info(
                            "try to transfer file for not allowed copy object, task_info: [key: {}], [value: {}], exception: {}",
                            migrateCopyBucketRecordElement.buildKey(),
                            migrateCopyBucketRecordElement.buildValue(), e.toString());
                    transferFileForNotAllowedCopyObject(migrateCopyBucketRecordElement);
                    return;
                }
            }
            String printMsg = String.format("[fail] task_info: %s",
                    migrateCopyBucketRecordElement.buildKey());
            System.out.println(printMsg);
            log.error("fail! task_info: [key: {}], [value: {}], exception: {}",
                    migrateCopyBucketRecordElement.buildKey(),
                    migrateCopyBucketRecordElement.buildValue(), e.toString());
            TaskStatics.instance.addFailCnt();
        }
    }


}
