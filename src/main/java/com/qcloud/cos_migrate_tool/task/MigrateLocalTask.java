package com.qcloud.cos_migrate_tool.task;

import java.io.File;
import java.util.concurrent.Semaphore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.qcloud.cos.model.StorageClass;
import com.qcloud.cos.transfer.TransferManager;
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


    public MigrateLocalTask(String bucketName, String localFolder, String cosFolder, File localFile,
            StorageClass storageClass, TransferManager smallFileTransfer,
            TransferManager bigFileTransfer, long smallFileThreshold, RecordDb recordDb, Semaphore semaphore,  boolean entireMd5Attached) {
        super(semaphore, smallFileTransfer, bigFileTransfer, smallFileThreshold, recordDb);
        this.bucketName = bucketName;
        this.localFolder = localFolder;
        this.cosFolder = cosFolder;
        this.localFile = localFile;
        this.storageClass = storageClass;
        this.entireMd5Attached = entireMd5Attached;
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

        MigrateLocalRecordElement migrateLocalRecordElement =
                new MigrateLocalRecordElement(bucketName, localPath, cosPath, mtime, fileSize);
        // 如果记录存在
        if (isExist(migrateLocalRecordElement)) {
            TaskStatics.instance.addSkipCnt();
            return;
        }

		try {
			uploadFile(bucketName, cosPath, localFile, storageClass, entireMd5Attached);
            saveRecord(migrateLocalRecordElement);
			TaskStatics.instance.addSuccessCnt();
			String printMsg = String.format("[ok] task_info: %s", migrateLocalRecordElement.buildKey());
			System.out.println(printMsg);
			log.info(printMsg);
		} catch (Exception e) {
			String printMsg = String.format("[fail] task_info: %s", migrateLocalRecordElement.buildKey());
			System.out.println(printMsg);
			log.error("fail! task_info: [key: {}], [value: {}], exception: {}", migrateLocalRecordElement.buildKey(),
					migrateLocalRecordElement.buildValue(), e.toString());
			TaskStatics.instance.addFailCnt();
		}
		
        /*
        PutObjectRequest putObjectRequest = new PutObjectRequest(bucketName, cosPath, localFile);
        putObjectRequest.setStorageClass(storageClass);
        Upload upload = null;
        try {
            if (fileSize >= SMALL_FILE_THRESHOLD) {
                upload = bigFileTransfer.upload(putObjectRequest);
            } else {
                upload = smallFileTransfer.upload(putObjectRequest);
            }
            upload.waitForCompletion();
            recordDb.saveRecord(migrateLocalRecordElement);
            TaskStatics.instance.addSuccessCnt();
            String printMsg =
                    String.format("[ok] task_info: %s", migrateLocalRecordElement.buildKey());
            System.out.println(printMsg);
        } catch (Exception e) {
            String printMsg =
                    String.format("[fail] task_info: %s", migrateLocalRecordElement.buildKey());
            System.out.println(printMsg);
            log.error("fail! task_info: [key: {}], [value: {}], exception: {}",
                    migrateLocalRecordElement.buildKey(), migrateLocalRecordElement.buildValue(),
                    e.toString());
            TaskStatics.instance.addSkipCnt();
        }
        */
    }
}
