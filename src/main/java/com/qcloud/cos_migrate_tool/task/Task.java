package com.qcloud.cos_migrate_tool.task;

import java.io.File;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.qcloud.cos.COSClient;
import com.qcloud.cos.exception.CosServiceException;
import com.qcloud.cos.model.ListMultipartUploadsRequest;
import com.qcloud.cos.model.ListPartsRequest;
import com.qcloud.cos.model.ObjectMetadata;
import com.qcloud.cos.model.PutObjectRequest;
import com.qcloud.cos.model.StorageClass;
import com.qcloud.cos.transfer.PersistableUpload;
import com.qcloud.cos.transfer.Transfer.TransferState;
import com.qcloud.cos.transfer.TransferManager;
import com.qcloud.cos.transfer.TransferProgress;
import com.qcloud.cos.transfer.Upload;
import com.qcloud.cos.utils.Md5Utils;
import com.qcloud.cos_migrate_tool.config.CommonConfig;
import com.qcloud.cos_migrate_tool.record.RecordDb;
import com.qcloud.cos_migrate_tool.record.RecordElement;

public abstract class Task implements Runnable {
    private Semaphore semaphore;
    public static final Logger log = LoggerFactory.getLogger(MigrateLocalTask.class);


    protected TransferManager smallFileTransfer;
    protected TransferManager bigFileTransfer;
    protected long smallFileThreshold;
    private RecordDb recordDb;
    protected CommonConfig config;

    public Task(Semaphore semaphore, CommonConfig config, TransferManager smallFileTransfer,
            TransferManager bigFileTransfer, RecordDb recordDb) {
        super();
        this.semaphore = semaphore;
        this.config = config;
        this.smallFileTransfer = smallFileTransfer;
        this.bigFileTransfer = bigFileTransfer;
        this.smallFileThreshold = config.getSmallFileThreshold();
        this.recordDb = recordDb;
    }

    public boolean isExist(RecordElement recordElement) {
        if (recordDb.queryRecord(recordElement)) {
            String printMsg = String.format("[skip] task_info: %s", recordElement.buildKey());
            System.out.println(printMsg);
            log.info("skip! task_info: [key: {}], [value: {}]", recordElement.buildKey(),
                    recordElement.buildValue());
            return true;
        }
        return false;
    }

    public void saveRecord(RecordElement recordElement) {
        recordDb.saveRecord(recordElement);
    }

    public void showTransferProgress(Upload upload, boolean multipart, String key, long mtime)
            throws InterruptedException {
        boolean pointSaveFlag = false;
        int printCount = 0;
        do {
            ++printCount;
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                return;
            }
            TransferProgress progress = upload.getProgress();
            long byteSent = progress.getBytesTransferred();
            long byteTotal = progress.getTotalBytesToTransfer();
            double pct = 100.0;
            if (byteTotal != 0) {
                pct = progress.getPercentTransferred();
            }
            if (printCount % 5 == 0) {
                String printMsg = String.format(
                        "[UploadInProgress] [key: %s] [byteSent/ byteTotal/ percentage: %d/ %d/ %.2f%%]",
                        key, byteSent, byteTotal, pct);
                log.info(printMsg);
                System.out.println(printMsg);
            }
            if (multipart && byteSent > 0 && !pointSaveFlag) {

                PersistableUpload persistableUploadInfo = upload.getResumeableMultipartUploadId();
                String multipartUploadId = null;
                if (persistableUploadInfo != null) {
                    multipartUploadId = persistableUploadInfo.getMultipartUploadId();
                    if (multipartUploadId != null) {
                        pointSaveFlag = this.recordDb.updateMultipartUploadSavePoint(
                                persistableUploadInfo.getBucketName(),
                                persistableUploadInfo.getKey(), persistableUploadInfo.getFile(),
                                mtime, persistableUploadInfo.getPartSize(),
                                persistableUploadInfo.getMutlipartUploadThreshold(),
                                persistableUploadInfo.getMultipartUploadId());
                        if (pointSaveFlag) {
                            log.info("save point success for multipart upload, key: {}", key);
                        } else {
                            log.error("save point failed for multipart upload, key: {}", key);
                        }
                    }

                }
            }

        } while (upload.isDone() == false);
        // 传输完成, 删除断点信息
        if (upload.getState() == TransferState.Completed && pointSaveFlag) {
            PersistableUpload persistableUploadInfo = upload.getResumeableMultipartUploadId();
            String multipartUploadId = null;
            if (persistableUploadInfo != null) {
                multipartUploadId = persistableUploadInfo.getMultipartUploadId();
                if (multipartUploadId != null) {
                    boolean deleteFlag = this.recordDb.deleteMultipartUploadSavePoint(
                            persistableUploadInfo.getBucketName(), persistableUploadInfo.getKey(),
                            persistableUploadInfo.getFile(), mtime,
                            persistableUploadInfo.getPartSize(),
                            persistableUploadInfo.getMutlipartUploadThreshold());
                    if (deleteFlag) {
                        log.info("delete point success for multipart upload, key: {}", key);
                    } else {
                        log.info("delete point failed for multipart upload, key: {}", key);
                    }
                }
            }
        }
        upload.waitForUploadResult();
    }

    private boolean isMultipartUploadIdValid(String bucketName, String cosKey, String uploadId) {
        ListPartsRequest listPartsRequest = new ListPartsRequest(bucketName, cosKey, uploadId);
        try {
            this.bigFileTransfer.getCOSClient().listParts(listPartsRequest);
            return true;
        } catch (CosServiceException cse) {
            return false;
        }

    }

    private void uploadBigFile(PutObjectRequest putObjectRequest) throws InterruptedException {
        String bucketName = putObjectRequest.getBucketName();
        String cosKey = putObjectRequest.getKey();
        String localPath = putObjectRequest.getFile().getAbsolutePath();
        long mtime = putObjectRequest.getFile().lastModified();
        long partSize = this.bigFileTransfer.getConfiguration().getMinimumUploadPartSize();
        long mutlipartUploadThreshold =
                this.bigFileTransfer.getConfiguration().getMultipartUploadThreshold();

        String multipartId = this.recordDb.queryMultipartUploadSavePoint(bucketName, cosKey,
                localPath, mtime, partSize, mutlipartUploadThreshold);
        Upload upload = null;
        // 如果multipartId不为Null, 则表示存在断点, 使用续传.
        if (multipartId != null && isMultipartUploadIdValid(bucketName, cosKey, multipartId)) {
            PersistableUpload persistableUpload = new PersistableUpload(bucketName, cosKey,
                    localPath, multipartId, partSize, mutlipartUploadThreshold);
            upload = this.bigFileTransfer.resumeUpload(persistableUpload);
        } else {
            upload = this.bigFileTransfer.upload(putObjectRequest);
        }
        showTransferProgress(upload, true, cosKey, mtime);
    }



    private void uploadSmallFile(PutObjectRequest putObjectRequest) throws InterruptedException {
        Upload upload = smallFileTransfer.upload(putObjectRequest);
        showTransferProgress(upload, false, putObjectRequest.getKey(),
                putObjectRequest.getFile().lastModified());
    }


    public void uploadFile(String bucketName, String cosPath, File localFile,
            StorageClass storageClass, boolean entireMd5Attached, Map<String, String> userMetaMap)
                    throws Exception {
        PutObjectRequest putObjectRequest = new PutObjectRequest(bucketName, cosPath, localFile);
        putObjectRequest.setStorageClass(storageClass);
        ObjectMetadata objectMetadata = new ObjectMetadata();
        if (userMetaMap != null) {
            objectMetadata.setUserMetadata(userMetaMap);
        }

        if (entireMd5Attached) {
            String md5 = Md5Utils.md5Hex(localFile);
            objectMetadata.addUserMetadata("md5", md5);
        }

        putObjectRequest.setMetadata(objectMetadata);

        int retryTime = 0;
        final int maxRetry = 5;

        while (retryTime < maxRetry) {
            try {
                if (localFile.length() >= smallFileThreshold) {
                    uploadBigFile(putObjectRequest);
                } else {
                    uploadSmallFile(putObjectRequest);
                }
                return;
            } catch (Exception e) {
                log.warn("upload failed, ready to retry. retryTime:" + retryTime, e);
                ++retryTime;
                if (retryTime >= maxRetry) {
                    throw e;
                } else {
                    Thread.sleep(ThreadLocalRandom.current().nextLong(200, 1000));
                }
            }
        }
    }

    public abstract void doTask();

    private void checkTimeWindows() throws InterruptedException {
        int timeWindowBegin = config.getTimeWindowBegin();
        int timeWindowEnd = config.getTimeWindowEnd();
        while (true) {
            DateTime dateTime = DateTime.now();
            int minuteOfDay = dateTime.getMinuteOfDay();
            if (minuteOfDay >= timeWindowBegin && minuteOfDay <= timeWindowEnd) {
                return;
            }
            String printTips =
                    String.format("currentTime %s, wait next time window [%02d:%02d, %02d:%02d]",
                            dateTime.toString("yyyy-MM-dd HH:mm:ss"), timeWindowBegin / 60,
                            timeWindowBegin % 60, timeWindowEnd / 60, timeWindowEnd % 60);
            System.out.println(printTips);
            log.info(printTips);
            Thread.sleep(60000);
        }
    }

    public void run() {
        try {
            checkTimeWindows();
            doTask();
        } catch (InterruptedException e) {
            log.error("task is interrupted", e);
        } catch (Exception e) {
            log.error("unknown exception occur", e);
        } finally {
            semaphore.release();
        }
    }
}
