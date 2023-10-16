package com.qcloud.cos_migrate_tool.hadoop_fs_task;

import com.qcloud.cos.model.StorageClass;
import com.qcloud.cos.transfer.TransferManager;
import com.qcloud.cos_migrate_tool.config.CopyFromLocalToCosnConfig;
import com.qcloud.cos_migrate_tool.meta.TaskStatics;
import com.qcloud.cos_migrate_tool.record.MigrateLocalRecordElement;
import com.qcloud.cos_migrate_tool.record.RecordDb;
import com.qcloud.cos_migrate_tool.task.Task;
import com.qcloud.cos_migrate_tool.utils.SystemUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.Semaphore;

public class MigrateLocalToCosnFsTask extends Task {
    private static final Logger log = LoggerFactory.getLogger(MigrateLocalToCosnFsTask.class);


    protected FileSystem fs;
    private String bucketName;
    private String localFolder;
    private String cosFolder;
    private File localFile;
    private StorageClass storageClass;
    private boolean entireMd5Attached;
    private boolean isCheckLocalRecord;

    public MigrateLocalToCosnFsTask(Semaphore semaphore, CopyFromLocalToCosnConfig copyFromLocalConfig,
                                    TransferManager smallFileTransfer, TransferManager bigFileTransfer, RecordDb recordDb,
                                    File localFile, FileSystem fs) {
        super(semaphore, copyFromLocalConfig, smallFileTransfer, bigFileTransfer, recordDb);
        this.bucketName = copyFromLocalConfig.getBucketName();
        this.localFolder = copyFromLocalConfig.getLocalPath();
        this.cosFolder = copyFromLocalConfig.getCosPath();
        this.localFile = localFile;
        this.storageClass = copyFromLocalConfig.getStorageClass();
        this.entireMd5Attached = copyFromLocalConfig.isEntireFileMd5Attached();
        isCheckLocalRecord = copyFromLocalConfig.checkLocalRecord();
        this.fs = fs;
    }

    @Override
    public void doTask() {
        String localPath = SystemUtils.formatLocalPath(localFile.getPath());
        String cosPath = buildCOSPath(localPath);
        long mtime = localFile.lastModified();
        long fileSize = localFile.length();
        long ignoreModifiedTimeLessThan =
                ((CopyFromLocalToCosnConfig) config).getIgnoreModifiedTimeLessThan();
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

        if (isCheckLocalRecord) {
            // 如果记录存在
            if (isExist(migrateLocalRecordElement, true)) {
                TaskStatics.instance.addSkipCnt();
                return;
            }
        }

        if (config.skipSamePath()) {
            try {
                if (isExistOnCOS(smallFileTransfer, migrateLocalRecordElement, config.getBucketName(), cosPath)) {
                    TaskStatics.instance.addSkipCnt();
                    return;
                }
            } catch (Exception e) {
                String printMsg = String.format("[fail] task_info: %s", migrateLocalRecordElement.buildKey());
                System.err.println(printMsg);
                log.error("[fail] task_info: {}, exception: {}", migrateLocalRecordElement.buildKey(), e.toString());
                TaskStatics.instance.addFailCnt();
                return;
            }
        }

        try {
            uploadFile(cosPath, localFile);
            saveRecord(migrateLocalRecordElement);
            if (this.query_result == RecordDb.QUERY_RESULT.KEY_NOT_EXIST) {
                TaskStatics.instance.addSuccessCnt();
            } else {
                TaskStatics.instance.addUpdateCnt();
            }

            if(!config.getOutputFinishedFilePath().isEmpty()) {
                SimpleDateFormat dateFormat= new SimpleDateFormat("YYYY-MM-dd");//设置当前时间的格式，为年-月-日
                String file_name = dateFormat.format(new Date()) + ".out";
                String resultFile = config.getOutputFinishedFilePath() + file_name;
                try {
                    BufferedOutputStream bos =
                            new BufferedOutputStream(new FileOutputStream(resultFile, true));
                    String recordMsg =
                            String.format("%s\t%d\t%d\n", localFile.getAbsolutePath(), localFile.length(), localFile.lastModified());
                    bos.write(recordMsg.getBytes());
                    bos.close();
                } catch (FileNotFoundException e) {
                    log.error("write result fail,result \n" + e.toString());
                } catch (IOException e) {
                    log.error("write result fail,result \n"  + e.toString());
                }
            }


            String printMsg =
                    String.format("[ok] , task_info: %s", migrateLocalRecordElement.buildKey());
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

    private String buildCOSPath(String localPath) {
        String cosPath = cosFolder + localPath.substring(localFolder.length());
        return cosPath;
    }

    public void uploadFile(String cosPath, File localFile) throws Exception {
        Path dstPath = new Path(cosPath);
        if(localFile.isDirectory()) {
            fs.mkdirs(dstPath);
        } else {
            Path localPath = new Path(localFile.getPath());
            fs.copyFromLocalFile(localPath, dstPath);
        }
    }
}
