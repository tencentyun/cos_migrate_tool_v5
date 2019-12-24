package com.qcloud.cos_migrate_tool.task;

import java.io.File;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;

import com.qcloud.cos.model.ObjectMetadata;
import com.qcloud.cos.transfer.TransferManager;
import com.qcloud.cos_migrate_tool.config.CopyFromQiniuConfig;
import com.qcloud.cos_migrate_tool.config.MigrateType;
import com.qcloud.cos_migrate_tool.meta.TaskStatics;
import com.qcloud.cos_migrate_tool.record.MigrateCompetitorRecordElement;
import com.qcloud.cos_migrate_tool.record.RecordDb;
import com.qcloud.cos_migrate_tool.utils.Downloader;
import com.qcloud.cos_migrate_tool.utils.HeadAttr;
import com.qiniu.util.Auth;

public class MigrateQiniuTask extends Task {
    private Auth auth;
    private String srcKey;
    private long fileSize;
    private String etag;

    public MigrateQiniuTask(CopyFromQiniuConfig config, Auth auth, String srcKey, long fileSize,
            String etag, TransferManager smallFileTransfer, TransferManager bigFileTransfer,
            RecordDb recordDb, Semaphore semaphore) {
        super(semaphore, config, smallFileTransfer, bigFileTransfer, recordDb);
        this.config = config;
        this.srcKey = srcKey;
        this.fileSize = fileSize;
        this.etag = etag;
        this.auth = auth;
    }

    private String buildCOSPath() {
        String srcPrefix = ((CopyFromQiniuConfig) config).getSrcPrefix();
        int lastDelimiter = srcPrefix.lastIndexOf("/");
        if (lastDelimiter == 0) {
            lastDelimiter = -1;
        }
        String keyName = srcKey.substring(lastDelimiter + 1);

        StringBuffer cosPathBuffer = new StringBuffer();
        cosPathBuffer.append(config.getCosPath()).append("/").append(keyName);

        String cosPath = cosPathBuffer.toString().replaceAll("/{2,}", "/");
        return cosPath;
    }

    @Override
    public void doTask() {

        String cosPath = buildCOSPath();

        String localPath = config.getTempFolderPath() + ThreadLocalRandom.current().nextLong();

        MigrateCompetitorRecordElement qiniuRecordElement = new MigrateCompetitorRecordElement(
                MigrateType.MIGRATE_FROM_QINIU, config.getBucketName(), cosPath, etag, fileSize);
        if (isExist(qiniuRecordElement, true)) {
            TaskStatics.instance.addSkipCnt();
            return;
        }

        // generate download url
        String url = "http://" + ((CopyFromQiniuConfig) config).getSrcEndpoint() + "/" + srcKey;
        if (((CopyFromQiniuConfig) config).IsNeedSign()) {
            url = auth.privateDownloadUrl(url, 3600);
        }

        File localFile = new File(localPath);

        // download
        HeadAttr headAttr = null;
        try {
            headAttr = Downloader.instance.downFile(url, localFile, true);
        } catch (Exception e) {
            TaskStatics.instance.addFailCnt();
            log.error("download fail url:{} msg:{}", url, e.getMessage());
            localFile.deleteOnExit();
            return;
        }

        if (headAttr == null) {
            log.error("download fail url:{}", url);
            TaskStatics.instance.addFailCnt();
            return;
        }

        // upload
        if (!localFile.exists()) {
            String errMsg = String.format("[fail] taskInfo: %s. file: %s not exist, srcKey: %s",
                    qiniuRecordElement.buildKey(), localPath, srcKey);
            System.err.println(errMsg);
            log.error(errMsg);
            TaskStatics.instance.addFailCnt();
            return;
        }

        if (localFile.length() != this.fileSize) {
            log.error("download size[{}] != list size[{}]", localFile.length(), this.fileSize);
            TaskStatics.instance.addFailCnt();
            return;
        }

        try {
            com.qcloud.cos.model.ObjectMetadata objectMetadata =
                    new com.qcloud.cos.model.ObjectMetadata();
            if (headAttr.userMetaMap.containsKey("ETag")) {
                objectMetadata.addUserMetadata("qiniu-etag", headAttr.userMetaMap.get("ETag"));
            }

            String requestId = uploadFile(config.getBucketName(), cosPath, localFile,
                    config.getStorageClass(), config.isEntireFileMd5Attached(), objectMetadata, null);
            saveRecord(qiniuRecordElement);
            saveRequestId(cosPath, requestId);
            if (this.query_result == RecordDb.QUERY_RESULT.KEY_NOT_EXIST) {
                TaskStatics.instance.addSuccessCnt();
            } else {
                TaskStatics.instance.addUpdateCnt();
            }
            String printMsg = String.format("[ok] [requestid: %s], task_info: %s",
                    requestId == null ? "NULL" : requestId, qiniuRecordElement.buildKey());
            System.out.println(printMsg);
            log.info(printMsg);
        } catch (Exception e) {
            String printMsg = String.format("[fail] task_info: %s", qiniuRecordElement.buildKey());
            System.err.println(printMsg);
            log.error("[fail] task_info: {}, exception: {}", qiniuRecordElement.buildKey(),
                    e.toString());
            TaskStatics.instance.addFailCnt();
        } finally {
            localFile.delete();
        }
    }
}
