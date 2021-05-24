package com.qcloud.cos_migrate_tool.task;

import java.io.File;
import java.util.UUID;
import java.util.concurrent.Semaphore;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.event.ProgressEvent;
import com.amazonaws.event.ProgressEventType;
import com.amazonaws.event.ProgressListener;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.qcloud.cos.transfer.TransferManager;
import com.qcloud.cos_migrate_tool.config.CopyFromAwsConfig;
import com.qcloud.cos_migrate_tool.config.MigrateType;
import com.qcloud.cos_migrate_tool.meta.TaskStatics;
import com.qcloud.cos_migrate_tool.record.MigrateCompetitorRecordElement;
import com.qcloud.cos_migrate_tool.record.RecordDb;

public class MigrateAwsTask extends Task {

    private String srcKey;
    private long fileSize;
    private String etag;
    private AmazonS3 s3Client;

    public MigrateAwsTask(CopyFromAwsConfig config, AmazonS3 s3Client, String srcKey, long fileSize,
            String etag, TransferManager smallFileTransfer, TransferManager bigFileTransfer,
            RecordDb recordDb, Semaphore semaphore) {
        super(semaphore, config, smallFileTransfer, bigFileTransfer, recordDb);
        this.s3Client = s3Client;
        this.srcKey = srcKey;
        this.fileSize = fileSize;
        this.etag = etag;
        if (srcKey.startsWith("/")) {
            this.srcKey = srcKey.substring(1);
        }
    }

    private String buildCOSPath() {
        String srcPrefix = ((CopyFromAwsConfig) config).getSrcPrefix();
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

        MigrateCompetitorRecordElement awsRecordElement = new MigrateCompetitorRecordElement(
                MigrateType.MIGRATE_FROM_AWS, config.getBucketName(), cosPath, etag, fileSize);
        if (isExist(awsRecordElement, true)) {
            TaskStatics.instance.addSkipCnt();
            return;
        }

        if (config.skipSamePath()) {
            try {
                if (isExistOnCOS(smallFileTransfer, MigrateType.MIGRATE_FROM_AWS, config.getBucketName(), cosPath)) {
                    TaskStatics.instance.addSkipCnt();
                    return;
                }
            } catch (Exception e) {
                String printMsg = String.format("[fail] task_info: %s", awsRecordElement.buildKey());
                System.err.println(printMsg);
                log.error("[fail] task_info: {}, exception: {}", awsRecordElement.buildKey(), e.toString());
                TaskStatics.instance.addFailCnt();
                return;
            }
        }

        // download
        // 下载object到文件
        String localPath = config.getTempFolderPath() + UUID.randomUUID().toString();
        File localFile = new File(localPath);
        ObjectMetadata awsMetaData = null;
        try {
            GetObjectProgressListener getObjectProgressListener =
                    new GetObjectProgressListener(srcKey);
            awsMetaData = s3Client.getObject(
                    new GetObjectRequest(((CopyFromAwsConfig) config).getSrcBucket(), srcKey)
                            .<GetObjectRequest>withGeneralProgressListener(
                                    getObjectProgressListener),
                    localFile);
            if (!localFile.exists()) {
                String printMsg =
                        String.format("[fail] [task_info: %s]", awsRecordElement.buildKey());
                System.out.println(printMsg);
                log.error(
                        "[fail] [taskInfo: {}] [srcKey: {}] [download localfile failed, localFile {} not exist]",
                        awsRecordElement.buildKey(), srcKey, localPath);
                TaskStatics.instance.addFailCnt();
                return;
            }

            if (localFile.length() != this.fileSize) {
                String printMsg =
                        String.format("[fail] [task_info: %s]", awsRecordElement.buildKey());
                System.out.println(printMsg);
                log.error("[fail] [taskInfo: {}] [download size {} not equal meta size {}]",
                        awsRecordElement.buildKey(), localFile.length(), this.fileSize);
                TaskStatics.instance.addFailCnt();
                localFile.delete();
                return;
            }

        } catch (AmazonServiceException e) {
            String printMsg = String.format("[fail] [task_info: %s]", awsRecordElement.buildKey());
            System.out.println(printMsg);
            log.error("[fail] [taskInfo: {}] [AmazonServiceException occur, msg {}]",
                    awsRecordElement.buildKey(), e.getMessage().toString());
            TaskStatics.instance.addFailCnt();
            localFile.delete();
            return;
        } catch (AmazonClientException e) {
            String printMsg = String.format("[fail] [task_info: %s]", awsRecordElement.buildKey());
            System.out.println(printMsg);
            log.error("[fail] [taskInfo: {}] [AmazonClientException occur, msg {}]",
                    awsRecordElement.buildKey(), e.getMessage().toString());
            TaskStatics.instance.addFailCnt();
            localFile.delete();
            return;
        }

        // upload file
        try {
            com.qcloud.cos.model.ObjectMetadata cosMetadata = new com.qcloud.cos.model.ObjectMetadata();
            if (awsMetaData.getUserMetadata() != null) {
                cosMetadata.setUserMetadata(awsMetaData.getUserMetadata());
            }
            if (awsMetaData.getCacheControl() != null) {
                cosMetadata.setCacheControl(awsMetaData.getCacheControl());
            }
            if (awsMetaData.getContentDisposition() != null) {
                cosMetadata.setContentDisposition(awsMetaData.getContentDisposition());
            }
            if (awsMetaData.getContentEncoding() != null) {
                cosMetadata.setContentEncoding(awsMetaData.getContentEncoding());
            }
            if (awsMetaData.getContentLanguage() != null) {
                cosMetadata.setContentLanguage(awsMetaData.getContentLanguage());
            }
            if (awsMetaData.getContentType() != null) {
                cosMetadata.setContentType(awsMetaData.getContentType());
            }
            if (awsMetaData.getETag() != null) {
                cosMetadata.addUserMetadata("s3-etag", awsMetaData.getETag());
            }
            String requestId = uploadFile(config.getBucketName(), cosPath, localFile,
                    config.getStorageClass(), config.isEntireFileMd5Attached(), cosMetadata, null);
            saveRecord(awsRecordElement);
            saveRequestId(cosPath, requestId);
            if (this.query_result == RecordDb.QUERY_RESULT.KEY_NOT_EXIST) {
                TaskStatics.instance.addSuccessCnt();
            } else {
                TaskStatics.instance.addUpdateCnt();
            }
            String printMsg = String.format("[ok] [requestid: %s], task_info: %s", requestId == null ? "NULL" : requestId, awsRecordElement.buildKey());
            System.out.println(printMsg);
            log.info(printMsg);
        } catch (Exception e) {
            String printMsg = String.format("[fail] [task_info: %s]", awsRecordElement.buildKey());
            System.out.println(printMsg);
            log.error("[fail] task_info: [key: {}], [value: {}], [exception: {}]",
                    awsRecordElement.buildKey(), awsRecordElement.buildValue(), e.toString());
            TaskStatics.instance.addFailCnt();
        } finally {
            localFile.delete();
        }

    }
}
