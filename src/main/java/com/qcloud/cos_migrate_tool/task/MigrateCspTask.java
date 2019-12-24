package com.qcloud.cos_migrate_tool.task;

import java.io.File;
import java.net.URL;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Semaphore;

import com.qcloud.cos.COSClient;
import com.qcloud.cos.event.ProgressEvent;
import com.qcloud.cos.event.ProgressEventType;
import com.qcloud.cos.event.ProgressListener;
import com.qcloud.cos.http.HttpMethodName;
import com.qcloud.cos.model.AccessControlList;
import com.qcloud.cos.model.GeneratePresignedUrlRequest;
import com.qcloud.cos.model.GetObjectRequest;
import com.qcloud.cos.model.Grant;
import com.qcloud.cos.model.ObjectMetadata;
import com.qcloud.cos.model.Permission;
import com.qcloud.cos.transfer.TransferManager;
import com.qcloud.cos_migrate_tool.config.CopyFromCspConfig;
import com.qcloud.cos_migrate_tool.config.MigrateType;
import com.qcloud.cos_migrate_tool.meta.TaskStatics;
import com.qcloud.cos_migrate_tool.record.MigrateCompetitorRecordElement;
import com.qcloud.cos_migrate_tool.record.RecordDb;
import com.qcloud.cos_migrate_tool.utils.TceGrantee;

public class MigrateCspTask extends Task {

    private String srcKey;
    private long fileSize;
    private String etag;
    private COSClient cosClient;

    public MigrateCspTask(CopyFromCspConfig config, COSClient cosClient, String srcKey,
            long fileSize, String etag, TransferManager smallFileTransfer,
            TransferManager bigFileTransfer, RecordDb recordDb, Semaphore semaphore) {
        super(semaphore, config, smallFileTransfer, bigFileTransfer, recordDb);
        this.cosClient = cosClient;
        this.srcKey = srcKey;
        this.fileSize = fileSize;
        this.etag = etag;
        if (srcKey.startsWith("/")) {
            this.srcKey = srcKey.substring(1);
        }
    }

    private String buildCOSPath() {
        String srcPrefix = ((CopyFromCspConfig) config).getSrcPrefix();
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

    /**
     * The downloading progress listener. Its progressChanged API is called by the SDK when there's
     * an update.
     */
    private static class GetObjectProgressListener implements ProgressListener {

        private String srcKey;

        private long bytesRead = 0;
        private long totalBytes = -1;
        private boolean succeed = false;
        private long lastPrintTimeStamp = 0;
        private long byteReadLastPrint = 0;

        public GetObjectProgressListener(String srcKey) {
            super();
            this.srcKey = srcKey;
        }

        private void showDownloadProgress(String key, long byteTotal, long byteDownloadSofar) {
            double pct = 100.0;
            if (byteTotal > 0) {
                pct = byteDownloadSofar * 1.0 / byteTotal * 100;
            }
            String status = "DownloadInProgress";
            if (byteTotal == byteDownloadSofar) {
                status = "DownloadOk";
            }
            String printMsg = String.format(
                    "[%s] [key: %s] [byteDownload/ byteTotal/ percentage: %d/ %d/ %.2f%%]", status,
                    key, byteDownloadSofar, byteTotal, pct);
            System.out.println(printMsg);
            log.info(printMsg);
        }

        public void progressChanged(ProgressEvent progressEvent) {
            long bytes = progressEvent.getBytes();
            ProgressEventType eventType = progressEvent.getEventType();
            switch (eventType) {
                case RESPONSE_CONTENT_LENGTH_EVENT:
                    this.totalBytes = bytes;
                    break;
                case RESPONSE_BYTE_TRANSFER_EVENT:
                    this.bytesRead += bytes;
                    if (this.bytesRead - this.byteReadLastPrint >= 1024) {
                        long currentTimeStamp = System.currentTimeMillis();
                        if (currentTimeStamp - lastPrintTimeStamp >= 2000) {
                            showDownloadProgress(srcKey, totalBytes, bytesRead);
                            byteReadLastPrint = bytesRead;
                            lastPrintTimeStamp = currentTimeStamp;
                        }
                    }
                    break;
                case TRANSFER_COMPLETED_EVENT:
                    this.succeed = true;
                    showDownloadProgress(srcKey, totalBytes, bytesRead);
                    break;
                default:
                    break;
            }
        }

        public boolean isSucceed() {
            return succeed;
        }

    }

    @Override
    public void doTask() {

        String cosPath = buildCOSPath();
        ObjectMetadata srcMetaData = null;

        if (this.etag.isEmpty()) {
            try {
                srcMetaData = cosClient
                        .getObjectMetadata(((CopyFromCspConfig) config).getSrcBucket(), srcKey);
                this.etag = srcMetaData.getETag();
                this.fileSize = srcMetaData.getContentLength();
            } catch (Exception e) {
                log.error("[fail] [taskInfo: {}] [Head Obj Exception occur, msg {}]", srcKey,
                        e.getMessage().toString());
                TaskStatics.instance.addFailCnt();
                return;
            }
        }

        MigrateCompetitorRecordElement cspRecordElement = new MigrateCompetitorRecordElement(
                MigrateType.MIGRATE_FROM_CSP, config.getBucketName(), cosPath, etag, fileSize);
        if (isExist(cspRecordElement, true)) {
            TaskStatics.instance.addSkipCnt();
            return;
        }

        // download
        // 下载object到文件
        String localPath = config.getTempFolderPath() + UUID.randomUUID().toString();
        File localFile = new File(localPath);
        try {
            GetObjectProgressListener getObjectProgressListener =
                    new GetObjectProgressListener(srcKey);
            srcMetaData = cosClient.getObject(
                    new GetObjectRequest(((CopyFromCspConfig) config).getSrcBucket(), srcKey)
                            .<GetObjectRequest>withGeneralProgressListener(
                                    getObjectProgressListener),
                    localFile);
           
            if (!localFile.exists()) {
                String printMsg =
                        String.format("[fail] [task_info: %s]", cspRecordElement.buildKey());
                System.out.println(printMsg);
                log.error(
                        "[fail] [taskInfo: {}] [srcKey: {}] [download localfile failed, localFile {} not exist]",
                        cspRecordElement.buildKey(), srcKey, localPath);
                TaskStatics.instance.addFailCnt();
                return;
            }

            if (localFile.length() != this.fileSize) {
                String printMsg =
                        String.format("[fail] [task_info: %s]", cspRecordElement.buildKey());
                System.out.println(printMsg);
                log.error("[fail] [taskInfo: {}] [download size {} not equal meta size {}]",
                        cspRecordElement.buildKey(), localFile.length(), this.fileSize);
                TaskStatics.instance.addFailCnt();
                localFile.delete();
                return;
            }

        } catch (Exception e) {
            String printMsg = String.format("[fail] [task_info: %s]", cspRecordElement.buildKey());
            System.out.println(printMsg);
            log.error("[fail] [taskInfo: {}] [Exception occur, msg {}]",
                    cspRecordElement.buildKey(), e.getMessage().toString());
            TaskStatics.instance.addFailCnt();
            localFile.delete();
            return;
        }

        
        AccessControlList acl;
        try {
            if(srcKey.endsWith("/") && !((CopyFromCspConfig) config).isMigrateSlashEndObjectAcl()) {
                // 如果key以'/'结尾, 且不迁移以'/'结尾key的acl, 则不去获取源的acl进行迁移
                acl = null;
            } else {
                acl = cosClient.getObjectAcl(((CopyFromCspConfig) config).getSrcBucket(), srcKey);
            }
        } catch (Exception e) {
            String printMsg = String.format("[fail] [task_info: %s]", cspRecordElement.buildKey());
            System.out.println(printMsg);
            log.error("[fail] [taskInfo: {}] [get acl Exception occur, msg {}]",
                    cspRecordElement.buildKey(), e.getMessage().toString());
            TaskStatics.instance.addFailCnt();
            localFile.delete();
            return;
        }

        // acl转换
        AccessControlList acl2 = new AccessControlList();
            
        if (acl != null) {
           
            List<Grant> grantList = acl.getGrantsAsList();
            
            for (int i = 0; i < grantList.size(); ++i) {
                if (grantList.get(i).getPermission() == Permission.Write) {
                    continue;
                }
                
                //System.out.printf("%s %s %s\n",srcKey, grantList.get(i).getGrantee().getTypeIdentifier(), grantList.get(i).getGrantee().getIdentifier());
                TceGrantee grantee = new TceGrantee();
                if (grantList.get(i).getGrantee().getTypeIdentifier().equalsIgnoreCase("uri")) {
               
                    
                    if (grantList.get(i).getGrantee().getIdentifier()
                            .equalsIgnoreCase("http://cam.qcloud.com/groups/global/AllUsers")) {
                

                        grantee.setIdentifier("qcs::cam::anonymous:anonymous");
                        
                    } else if (grantList.get(i).getGrantee().getIdentifier().equalsIgnoreCase(
                            "http://cam.qcloud.com/groups/global/AuthenticatedUsers")) {
                        grantee.setIdentifier("authenticated");
                    }
                    
                    grantee.setTypeIdentifier("id");

                } else {
                    grantee.setTypeIdentifier(grantList.get(i).getGrantee().getTypeIdentifier());
                    grantee.setIdentifier(grantList.get(i).getGrantee().getIdentifier());
                }
                
                acl2.grantPermission(grantee, grantList.get(i).getPermission());
            }
        }


        // upload file
        try {
            com.qcloud.cos.model.ObjectMetadata cosMetadata =
                    new com.qcloud.cos.model.ObjectMetadata();
            if (srcMetaData.getUserMetadata() != null) {
                cosMetadata.setUserMetadata(srcMetaData.getUserMetadata());
            }
            if (srcMetaData.getCacheControl() != null) {
                cosMetadata.setCacheControl(srcMetaData.getCacheControl());
            }
            if (srcMetaData.getContentDisposition() != null) {
                cosMetadata.setContentDisposition(srcMetaData.getContentDisposition());
            }
            if (srcMetaData.getContentEncoding() != null) {
                cosMetadata.setContentEncoding(srcMetaData.getContentEncoding());
            }
            if (srcMetaData.getContentLanguage() != null) {
                cosMetadata.setContentLanguage(srcMetaData.getContentLanguage());
            }
            if (srcMetaData.getContentType() != null) {
                cosMetadata.setContentType(srcMetaData.getContentType());
            }
            if (srcMetaData.getETag() != null) {
                cosMetadata.addUserMetadata("csp-etag", srcMetaData.getETag());
            }

            String requestId = uploadFile(config.getBucketName(), cosPath, localFile,
                    config.getStorageClass(), config.isEntireFileMd5Attached(), cosMetadata, acl2);
            saveRecord(cspRecordElement);
            saveRequestId(cosPath, requestId);
            if (this.query_result == RecordDb.QUERY_RESULT.KEY_NOT_EXIST) {
                TaskStatics.instance.addSuccessCnt();
            } else {
                TaskStatics.instance.addUpdateCnt();
            }
            String printMsg = String.format("[ok] [requestid: %s], task_info: %s",
                    requestId == null ? "NULL" : requestId, cspRecordElement.buildKey());
            System.out.println(printMsg);
            log.info(printMsg);
        } catch (Exception e) {
            String printMsg = String.format("[fail] [task_info: %s]", cspRecordElement.buildKey());
            System.out.println(printMsg);
            log.error("[fail] task_info: [key: {}], [value: {}], [exception: {}]",
                    cspRecordElement.buildKey(), cspRecordElement.buildValue(), e.toString());
            TaskStatics.instance.addFailCnt();
        } finally {
            localFile.delete();
        }

    }
}
