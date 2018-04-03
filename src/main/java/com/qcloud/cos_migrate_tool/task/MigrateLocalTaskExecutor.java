package com.qcloud.cos_migrate_tool.task;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;

import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.qcloud.cos.model.StorageClass;
import com.qcloud.cos_migrate_tool.config.CopyFromLocalConfig;
import com.qcloud.cos_migrate_tool.config.MigrateType;
import com.qcloud.cos_migrate_tool.utils.SystemUtils;

public class MigrateLocalTaskExecutor extends TaskExecutor {

    private static final Logger log = LoggerFactory.getLogger(MigrateLocalTaskExecutor.class);

    private String bucketName;
    private String localFolder;
    private String cosFolder;

    public MigrateLocalTaskExecutor(CopyFromLocalConfig config) {
        super(MigrateType.MIGRATE_FROM_LOCAL, config);
        this.bucketName = config.getBucketName();
        this.localFolder = config.getLocalPath();
        this.cosFolder = config.getCosPath();
    }

    @Override
    public String buildTaskDbComment() {
        String comment = String.format(
                "[time: %s], [bucketName: %s], [localFolder: %s], [cosFolder: %s], [smallfile_exector_number: %d], [bigfile_executor_number: %d]\n",
                SystemUtils.getCurrentDateTime(), bucketName, localFolder, cosFolder,
                this.smallFileUploadExecutorNum, this.bigFileUploadExecutorNum);
        return comment;
    }

    @Override
    public String buildTaskDbFolderPath() {
        String temp = String.format("[local: %s], [cosFolder: %s]", localFolder, cosFolder);
        String dbFolderPath =
                String.format("db/migrate_from_local/%s/%s", bucketName, DigestUtils.md5Hex(temp));
        return dbFolderPath;
    }

    public void buildTask() {
        SimpleFileVisitor<Path> finder = new SimpleFileVisitor<Path>() {

            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs)
                    throws IOException {
                String dirPath = SystemUtils.formatLocalPath(dir.toString());
                if (((CopyFromLocalConfig) config).isExcludes(dirPath)) {
                    return FileVisitResult.SKIP_SUBTREE;
                } else {
                    return super.preVisitDirectory(dir, attrs);
                }
            }

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
                    throws IOException {

                String localPath = SystemUtils.formatLocalPath(file.toString());
                String filePath = "";
                if (filePath.contains(".w.")) {
                    return super.visitFile(file, attrs);
                }
                try {
                    if (!((CopyFromLocalConfig) config).isExcludes(localPath)) {
                        File localFile = new File(file.toString());
                        StorageClass storageClass =
                                ((CopyFromLocalConfig) config).getStorageClass();
                        boolean entireFileMd5Attached = config.isEntireFileMd5Attached();
                        long smallFileThreshold = config.getSmallFileThreshold();
                        MigrateLocalTask migrateLocalTask = new MigrateLocalTask(bucketName,
                                localFolder, cosFolder, localFile, storageClass,
                                smallFileTransferManager, bigFileTransferManager,
                                smallFileThreshold, recordDb, semaphore, entireFileMd5Attached);
                        AddTask(migrateLocalTask);
                    }
                } catch (InterruptedException e) {
                    throw new IOException(e.getMessage());
                }
                return super.visitFile(file, attrs);
            }
        };

        try {
            java.nio.file.Files.walkFileTree(Paths.get(localFolder), finder);
        } catch (IOException e) {
            log.error("walk file tree error", e);
        }
    }

    @Override
    public void waitTaskOver() {
        super.waitTaskOver();
    }
}
