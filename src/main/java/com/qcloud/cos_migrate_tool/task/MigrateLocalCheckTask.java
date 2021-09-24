package com.qcloud.cos_migrate_tool.task;


import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.concurrent.Semaphore;

import com.qcloud.cos.model.ObjectMetadata;
import com.qcloud.cos.transfer.TransferManager;
import com.qcloud.cos.utils.CRC64;
import com.qcloud.cos_migrate_tool.config.CopyFromLocalConfig;
import com.qcloud.cos_migrate_tool.meta.TaskStatics;
import com.qcloud.cos_migrate_tool.record.RecordDb;
import com.qcloud.cos_migrate_tool.utils.SystemUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MigrateLocalCheckTask extends Task {
    private static final Logger log = LoggerFactory.getLogger(MigrateLocalTask.class);

    private String bucketName;
    private String localFolder;
    private String cosFolder;
    private File localFile;

    public MigrateLocalCheckTask(Semaphore semaphore, CopyFromLocalConfig copyFromLocalConfig,
            TransferManager smallFileTransfer, TransferManager bigFileTransfer, RecordDb recordDb,
            File localFile) {
        super(semaphore, copyFromLocalConfig, smallFileTransfer, bigFileTransfer, recordDb);
        this.bucketName = copyFromLocalConfig.getBucketName();
        this.localFolder = copyFromLocalConfig.getLocalPath();
        this.cosFolder = copyFromLocalConfig.getCosPath();
        this.localFile = localFile;
    }

    private String buildCOSPath(String localPath) {
        String cosPath = cosFolder + localPath.substring(localFolder.length());
        return cosPath;
    }

    @Override
    public void doTask() {
        String localPath = SystemUtils.formatLocalPath(localFile.getPath());
        String cosPath = buildCOSPath(localPath);
        try {
            ObjectMetadata cosMeta = this.smallFileTransfer.getCOSClient().getObjectMetadata(bucketName, cosPath); 
            //String localCrc64 = calculateCrc64(localFile);

            long localLen = localFile.length();
            long cosLen = cosMeta.getContentLength();

            if (localLen == cosLen) {
                TaskStatics.instance.addSuccessCnt();
            } else {
                String printMsg =
                        String.format("[fail] check length fail: %s. cosLength: %s, localLength: %s",
                            cosPath, cosLen, localLen);
                System.out.println(printMsg);
                log.error(printMsg);
                TaskStatics.instance.addFailCnt();
                return;
            }
            
            String printMsg =
                    String.format("[ok] check length ok: %s", cosPath);
            System.out.println(printMsg);
            log.info(printMsg);
        } catch (Exception e) {
            String printMsg =
                    String.format("[fail] check length fail: %s", cosPath);
            System.out.println(printMsg);
            log.error("fail! checkout length fail: %s, exception: %s",
                    cosPath,e.toString());
            TaskStatics.instance.addFailCnt();
        }
    }

    String calculateCrc64(File localFile) throws IOException {
        CRC64 crc64 = new CRC64();

        try (FileInputStream stream = new FileInputStream(localFile)) {
            byte[] b = new byte[1024 * 1024];
            while (true) {
                final int read = stream.read(b);
                if (read <= 0) {
                    break;
                }
                crc64.update(b, read);
            }
        }

        return Long.toUnsignedString(crc64.getValue());
    }
}
