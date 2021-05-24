package com.qcloud.cos_migrate_tool.task;

import java.io.File;
import java.io.FileInputStream;
import java.math.BigInteger;
import java.util.Date;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;

import com.aliyun.oss.OSSClient;
import com.aliyun.oss.common.utils.CRC64;
import com.aliyun.oss.event.ProgressEvent;
import com.aliyun.oss.event.ProgressEventType;
import com.aliyun.oss.event.ProgressListener;
import com.aliyun.oss.model.GetObjectRequest;
import com.aliyun.oss.model.ObjectMetadata;
import com.qcloud.cos.exception.CosServiceException;
import com.qcloud.cos.transfer.TransferManager;
import com.qcloud.cos_migrate_tool.config.CopyFromAliConfig;
import com.qcloud.cos_migrate_tool.config.MigrateType;
import com.qcloud.cos_migrate_tool.meta.TaskStatics;
import com.qcloud.cos_migrate_tool.record.MigrateCompetitorRecordElement;
import com.qcloud.cos_migrate_tool.record.RecordDb;

public class MigrateAliTask extends Task {

    private OSSClient ossClient;
    private String srcKey;
    private long fileSize;
    private String etag;
    private Date lastModify;

    public MigrateAliTask(CopyFromAliConfig config, OSSClient ossClient, String srcKey,
            long fileSize, String etag, Date lastModify, TransferManager smallFileTransfer,
            TransferManager bigFileTransfer, RecordDb recordDb, Semaphore semaphore) {
        super(semaphore, config, smallFileTransfer, bigFileTransfer, recordDb);
        this.ossClient = ossClient;
        this.srcKey = srcKey;
        this.fileSize = fileSize;
        this.etag = etag;
        this.lastModify = lastModify;
        if (srcKey.startsWith("/")) {
            this.srcKey = srcKey.substring(1);
        }

    }

    private String buildCOSPath() {
        String srcPrefix = ((CopyFromAliConfig) config).getSrcPrefix();
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
        String localPath = config.getTempFolderPath() + ThreadLocalRandom.current().nextLong();

        MigrateCompetitorRecordElement ossRecordElement = new MigrateCompetitorRecordElement(
                MigrateType.MIGRATE_FROM_ALI, config.getBucketName(), cosPath, etag, fileSize);

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
                    log.error("[fail] task_info: {}, exception: {}", ossRecordElement.buildKey(),
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
                log.error("[fail] task_info: {}, exception: {}", ossRecordElement.buildKey(),
                        e.toString());
                TaskStatics.instance.addFailCnt();
                return;
            }

        } else if (isExist(ossRecordElement, true)) {
            TaskStatics.instance.addSkipCnt();
            return;
        }

        if (config.skipSamePath()) {
            try {
                if (isExistOnCOS(smallFileTransfer, MigrateType.MIGRATE_FROM_ALI, config.getBucketName(), cosPath)) {
                    TaskStatics.instance.addSkipCnt();
                    return;
                }
            } catch (Exception e) {
                String printMsg = String.format("[fail] task_info: %s", ossRecordElement.buildKey());
                System.err.println(printMsg);
                log.error("[fail] task_info: {}, exception: {}", ossRecordElement.buildKey(), e.toString());
                TaskStatics.instance.addFailCnt();
                return;
            }
        }

        ObjectMetadata aliMetaData = null;
        try {
            // download
            // 下载object到文件
            GetObjectProgressListener downloadProgressListener =
                    new GetObjectProgressListener(srcKey);
            aliMetaData = ossClient.getObject(
                    new GetObjectRequest(((CopyFromAliConfig) config).getSrcBucket(), srcKey)
                            .<GetObjectRequest>withProgressListener(downloadProgressListener),
                    new File(localPath));
            if (!downloadProgressListener.isSucceed()) {
                throw new Exception("download from ali failed");
            }

            if (aliMetaData.getRawMetadata().get("x-oss-hash-crc64ecma") != null) {
                String serverChecksum =
                        aliMetaData.getRawMetadata().get("x-oss-hash-crc64ecma").toString().trim();

                if ((serverChecksum != null) && !serverChecksum.isEmpty()) {
                    FileInputStream stream = new FileInputStream(new File(localPath));
                    CRC64 crc = new CRC64();
                    byte[] b = new byte[65536];
                    int len = 0;

                    while ((len = stream.read(b)) != -1) {
                        crc.update(b, len);
                    }
                    stream.close();
                    BigInteger serverCrcNum = new BigInteger(serverChecksum);
                    BigInteger localCrcNum = new BigInteger(1, crc.getBytes());

                    if (!localCrcNum.equals(serverCrcNum)) {
                        String errMsg = String.format(
                                "[fail] taskInfo: %s, crc check fail, local crc: %s, server crc: %s",
                                ossRecordElement.buildKey(), localCrcNum.toString(),
                                serverCrcNum.toString());
                        System.err.println(errMsg);
                        log.error(errMsg);
                        File localFile;
                        localFile = new File(localPath);
                        if (localFile.exists()) {
                            localFile.delete();
                        }
                        TaskStatics.instance.addFailCnt();
                        return;
                    }
                }
            }

        } catch (Exception e) {
            String errMsg = String.format("[fail] taskInfo: %s, Caught an Exception, error msg: %s",
                    ossRecordElement.buildKey(), e.toString());
            System.err.println(errMsg);
            log.error(errMsg);
            TaskStatics.instance.addFailCnt();
            File localFile;
            localFile = new File(localPath);
            if (localFile.exists()) {
                localFile.delete();
            }
            return;
        }

        // upload
        File localFile;
        localFile = new File(localPath);

        if (!localFile.exists()) {
            String errMsg = String.format("[fail] taskInfo: %s. file: %s not exist, srcKey: %s",
                    ossRecordElement.buildKey(), localPath, srcKey);
            System.err.println(errMsg);
            log.error(errMsg);
            TaskStatics.instance.addFailCnt();
            return;
        }

        if (localFile.length() != this.fileSize) {
            String errMsg = String.format(
                    "[fail] taskInfo: %s, download file size %d not equal meta size %d",
                    ossRecordElement.buildKey(), localFile.length(), this.fileSize);
            System.err.println(errMsg);
            log.error(errMsg);
            TaskStatics.instance.addFailCnt();
            localFile.delete();
            return;
        }

        try {
            com.qcloud.cos.model.ObjectMetadata cosMetadata =
                    new com.qcloud.cos.model.ObjectMetadata();
            if (aliMetaData.getUserMetadata() != null) {
                cosMetadata.setUserMetadata(aliMetaData.getUserMetadata());
            }
            if (aliMetaData.getCacheControl() != null) {
                cosMetadata.setCacheControl(aliMetaData.getCacheControl());
            }
            if (aliMetaData.getContentDisposition() != null) {
                cosMetadata.setContentDisposition(aliMetaData.getContentDisposition());
            }
            if (aliMetaData.getContentEncoding() != null) {
                cosMetadata.setContentEncoding(aliMetaData.getContentEncoding());
            }
            if (aliMetaData.getContentType() != null) {
                cosMetadata.setContentType(aliMetaData.getContentType());
            }
            if (aliMetaData.getETag() != null) {
                cosMetadata.addUserMetadata("oss-etag", aliMetaData.getETag());
            }
            String requestId = uploadFile(config.getBucketName(), cosPath, localFile,
                    config.getStorageClass(), config.isEntireFileMd5Attached(), cosMetadata, null);
            saveRecord(ossRecordElement);
            saveRequestId(cosPath, requestId);
            if (this.query_result == RecordDb.QUERY_RESULT.KEY_NOT_EXIST) {
                TaskStatics.instance.addSuccessCnt();
            } else {
                TaskStatics.instance.addUpdateCnt();
            }
            String printMsg = String.format("[ok] [requestid: %s], task_info: %s",
                    requestId == null ? "NULL" : requestId, ossRecordElement.buildKey());
            System.out.println(printMsg);
            log.info(printMsg);
        } catch (Exception e) {
            String printMsg = String.format("[fail] task_info: %s", ossRecordElement.buildKey());
            System.err.println(printMsg);
            log.error("[fail] task_info: {}, exception: {}", ossRecordElement.buildKey(),
                    e.toString());
            TaskStatics.instance.addFailCnt();
        } finally {
            localFile.delete();
        }

    }
}
