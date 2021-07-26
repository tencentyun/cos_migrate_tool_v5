package com.qcloud.cos_migrate_tool.task;

import java.io.File;
import java.util.Date;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Semaphore;

import com.UpYun;
import com.qcloud.cos.exception.CosServiceException;
import com.qcloud.cos.transfer.TransferManager;
import com.qcloud.cos.utils.UrlEncoderUtils;
import com.qcloud.cos_migrate_tool.config.CopyFromUpyunConfig;
import com.qcloud.cos_migrate_tool.config.MigrateType;
import com.qcloud.cos_migrate_tool.meta.TaskStatics;
import com.qcloud.cos_migrate_tool.record.MigrateCompetitorRecordElement;
import com.qcloud.cos_migrate_tool.record.RecordDb;

public class MigrateUpyunTask extends Task {

    private UpYun upyun;
    private String srcKey;
    private long fileSize;
    private String etag = "";
    private Date lastModify;
    private String contentType;

    public MigrateUpyunTask(CopyFromUpyunConfig config, UpYun upyun, String srcKey, long fileSize,
            Date lastModify, String contentType, TransferManager smallFileTransfer,
            TransferManager bigFileTransfer, RecordDb recordDb, Semaphore semaphore) {
        super(semaphore, config, smallFileTransfer, bigFileTransfer, recordDb);
        //this.upyun = upyun;
        
        //又拍云sdk多线程有坑，headers不对
        this.upyun = new UpYun(config.getSrcBucket(), config.getSrcAccessKeyId(), config.getSrcAccessKeySecret());
        this.upyun.setTimeout(60);
        this.upyun.setApiDomain(UpYun.ED_AUTO);

        if (!config.getSrcProxyHost().isEmpty() && config.getSrcProxyPort() > 0) {
            System.setProperty("java.net.useSystemProxies", "true");
            System.setProperty("http.proxyHost", config.getSrcProxyHost());
            System.setProperty("http.proxyPort", Integer.toString(config.getSrcProxyPort()));
        }
        
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
        } else {
	    if (srcPrefix.startsWith("/")) {
                lastDelimiter = lastDelimiter - 1;
            }
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

                if (config.skipSamePath()) {
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

        if (config.skipSamePath()) {
            try {
                if (isExistOnCOS(smallFileTransfer, upyunRecordElement, config.getBucketName(), cosPath)) {
                    TaskStatics.instance.addSkipCnt();
                    return;
                }
            } catch (Exception e) {
                String printMsg = String.format("[fail] task_info: %s", upyunRecordElement.buildKey());
                System.err.println(printMsg);
                log.error("[fail] task_info: {}, exception: {}", upyunRecordElement.buildKey(), e.toString());
                TaskStatics.instance.addFailCnt();
                return;
            }
        }

        String localPath = config.getTempFolderPath() + UUID.randomUUID().toString();
        File localFile = new File(localPath);
        int retry_limit = 5;
        boolean download_success = false;
        String contentMd5 = "";

        do {
            try {
           
                if (((CopyFromUpyunConfig) config).isCompareMd5()) {
                    Map<String, String> headers = this.upyun.getFileInfo(UrlEncoderUtils.encodeEscapeDelimiter(this.srcKey));
                    
                    if (headers == null || !headers.containsKey("Content-MD5")) {
                        String errMsg = String
                                .format("[fail] taskInfo: %s, can't get fileinfo or content-md5", upyunRecordElement.buildKey());
                        System.err.println(errMsg);
                        log.error(errMsg);
                        TaskStatics.instance.addFailCnt();
                        return;
                    }

                    contentMd5 = headers.get("Content-Md5");
                }

                download_success = this.upyun
                        .readFile(UrlEncoderUtils.encodeEscapeDelimiter(this.srcKey), localFile);
                if (!download_success) {
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
                download_success = false;
                retry_limit--;

                try {
                    Thread.sleep(300);
                } catch (InterruptedException e1) {
                    // TODO Auto-generated catch block
                    e1.printStackTrace();
                }

                if (retry_limit == 0) {
                    String errMsg =
                            String.format("[fail] taskInfo: %s, Caught an Exception, error msg: %s",
                                    upyunRecordElement.buildKey(), e.toString());
                    System.err.println(errMsg);
                    log.error(errMsg);
                    TaskStatics.instance.addFailCnt();
                    if (localFile.exists()) {
                        localFile.delete();
                    }
                    return;
                }

                if (localFile.exists()) {
                    localFile.delete();
                }

            }


        } while (!download_success && retry_limit > 0);

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
        if (((CopyFromUpyunConfig) config).isCompareMd5()) {
            cosMetadata.addUserMetadata("upyun-etag", contentMd5);
        }
        
        cosMetadata.setContentType(contentType);
    
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
            String printMsg = String.format("[fail] task_info: %s exception: %s", upyunRecordElement.buildKey(), e.toString());
            System.err.println(printMsg);
            log.error(printMsg);
            TaskStatics.instance.addFailCnt();
        } finally {
            localFile.delete();
        }

    }
}
