package com.qcloud.cos_migrate_tool.task;

import java.io.File;
import java.util.concurrent.Semaphore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.qcloud.cos.model.StorageClass;
import com.qcloud.cos.transfer.TransferManager;
import com.qcloud.cos_migrate_tool.config.CopyFromLocalConfig;
import com.qcloud.cos_migrate_tool.meta.TaskStatics;
import com.qcloud.cos_migrate_tool.record.MigrateLocalRecordElement;
import com.qcloud.cos_migrate_tool.record.RecordDb;
import com.qcloud.cos_migrate_tool.utils.SystemUtils;

public class MigrateLocalTask extends Task {
    private static final Logger log = LoggerFactory.getLogger(MigrateLocalTask.class);

    private String bucketName;
    private String localFolder;
    private String cosFolder;
    private File localFile;
    private StorageClass storageClass;
    private boolean entireMd5Attached;


    public MigrateLocalTask(Semaphore semaphore, CopyFromLocalConfig copyFromLocalConfig,
            TransferManager smallFileTransfer, TransferManager bigFileTransfer, RecordDb recordDb,
            File localFile) {
        super(semaphore, copyFromLocalConfig, smallFileTransfer, bigFileTransfer, recordDb);
        this.bucketName = copyFromLocalConfig.getBucketName();
        this.localFolder = copyFromLocalConfig.getLocalPath();
        this.cosFolder = copyFromLocalConfig.getCosPath();
        this.localFile = localFile;
        this.storageClass = copyFromLocalConfig.getStorageClass();
        this.entireMd5Attached = copyFromLocalConfig.isEntireFileMd5Attached();
    }

    private String buildCOSPath(String localPath) {
        String cosPath = cosFolder + localPath.substring(localFolder.length());
        return cosPath;
    }


    @Override
    public void doTask() {
        String localPath = SystemUtils.formatLocalPath(localFile.getPath());
        String cosPath = buildCOSPath(localPath);
        long mtime = localFile.lastModified();
        long fileSize = localFile.length();
        long ignoreModifiedTimeLessThan =
                ((CopyFromLocalConfig) config).getIgnoreModifiedTimeLessThan();
        if (ignoreModifiedTimeLessThan > 0) {
            long currTime = System.currentTimeMillis();
            if ((currTime - mtime) / 1000 < ignoreModifiedTimeLessThan) {
                String printMsg = String.format(
                        "[condition_not_match] [reason: ignoreModifiedTimeLessThan]  [local_file: %s], [cur_time: %d], [lastModifed_time: %d]，[ignoreModifiedTimeLessThan: %d]",
                        localFile.getAbsoluteFile(), currTime / 1000, mtime / 1000, ignoreModifiedTimeLessThan);
                System.out.println(printMsg);
                log.info(printMsg);
                TaskStatics.instance.addConditionNotMatchCnt();
                return;
            }
        }

        MigrateLocalRecordElement migrateLocalRecordElement =
                new MigrateLocalRecordElement(bucketName, localPath, cosPath, mtime, fileSize);
        // 如果记录存在
        if (isExist(migrateLocalRecordElement)) {
            TaskStatics.instance.addSkipCnt();
            return;
        }

        try {
            uploadFile(bucketName, cosPath, localFile, storageClass, entireMd5Attached, null);
            saveRecord(migrateLocalRecordElement);
            TaskStatics.instance.addSuccessCnt();
            String printMsg =
                    String.format("[ok] task_info: %s", migrateLocalRecordElement.buildKey());
            System.out.println(printMsg);
            log.info(printMsg);
        } catch (Exception e) {
            String printMsg =
                    String.format("[fail] task_info: %s", migrateLocalRecordElement.buildKey());
            System.out.println(printMsg);
            log.error("fail! task_info: [key: {}], [value: {}], exception: {}",
                    migrateLocalRecordElement.buildKey(), migrateLocalRecordElement.buildValue(),
                    e.toString());
            TaskStatics.instance.addFailCnt();
        }
    }
}
