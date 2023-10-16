package com.qcloud.cos_migrate_tool.task;

import static java.nio.file.FileVisitOption.FOLLOW_LINKS;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.EnumSet;

import com.qcloud.cos_migrate_tool.config.CopyFromLocalConfig;
import com.qcloud.cos_migrate_tool.config.MigrateType;
import com.qcloud.cos_migrate_tool.meta.TaskStatics;
import com.qcloud.cos_migrate_tool.utils.SystemUtils;

import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MigrateLocalTaskExecutor extends TaskExecutor {

    private static final Logger log = LoggerFactory.getLogger(MigrateLocalTaskExecutor.class);

    private String bucketName;
    private String localFolder;
    private String cosFolder;
    private CopyFromLocalConfig config;

    public MigrateLocalTaskExecutor(CopyFromLocalConfig config) {
        super(MigrateType.MIGRATE_FROM_LOCAL, config);
        this.bucketName = config.getBucketName();
        this.localFolder = config.getLocalPath();
        this.cosFolder = config.getCosPath();
        this.config = config;
    }

    @Override
    public String buildTaskDbComment() {
        String comment = String.format(
                "[time: %s], [bucketName: %s], [localFolder: %s], [cosFolder: %s], [smallfile_exector_number: %d], [bigfile_executor_number: %d]\n",
                SystemUtils.getCurrentDateTime(), bucketName, localFolder, cosFolder,
                this.smallFileUploadExecutorNum, this.bigFileUploadExecutorNum);
        return comment;
    }

    /**
     * [md5path]是通过如下方式计算
     * 获取本地目录与cos上的目标目录，拼接出字符串“[local: %s], [cosFolder: %s]”，再做32位的md5
     *
     * 示例1，配置文件中：
     * localPath=/data/log
     * cosPath=/
     * 拼接出字符串如下，请注意如果不是以“/”结束，需要补齐1个“/”
     * [local: /data/log/], [cosFolder: /]
     *
     * 32位md5:
     * 0804c9153e80cb96474ef53cc31fb55a
     *
     * 示例2：
     * localPath=/data/log
     * cosPath=/aaa
     * [local: /data/log/], [cosFolder: /aaa/]
     *
     * 32位md5:
     * 3f3b01e32d9fe04259ca686fafe70117
     */
    @Override
    public String buildTaskDbFolderPath() {
        String temp = String.format("[local: %s], [cosFolder: %s]", localFolder, cosFolder);
        String dbFolderPath =
                String.format("db/migrate_from_local/%s/%s", bucketName, DigestUtils.md5Hex(temp));
        return dbFolderPath;
    }

    private void buildFileListTask() {
        String localPathPrefix = "";
        if(config.getLocalPath() != null) {
            localPathPrefix = config.getLocalPath();
        }

        try (BufferedReader br = new BufferedReader(new FileReader(new File(config.getFileListPath())))) {
            String line;
            while ((line = br.readLine()) != null) {
                Path file = Paths.get(localPathPrefix, line);
                String localPath = "";
                try {
                    localPath = SystemUtils.formatLocalPath(file.toString());
                } catch (IllegalArgumentException e) {
                    String printMsg = String.format("skip the file for illegal utf-8 letter, [local_file:%s]",
                            file.toString());
                    System.out.println(printMsg);
                    log.error(printMsg);
                    TaskStatics.instance.addConditionNotMatchCnt();
                    continue;
                }
                try {
                    String reason = config.needToMigrate(file, localPath);
                    if (reason.isEmpty()) {
                        File localFile = new File(file.toString());

                        MigrateLocalTask migrateLocalTask = new MigrateLocalTask(semaphore,
                                config, smallFileTransferManager,
                                bigFileTransferManager, recordDb, localFile);
                        AddTask(migrateLocalTask);
                    } else {
                        String printMsg = String.format(
                                "[condition_not_match] [reason: %s]  [local_file: %s]", reason,
                                file.toString());
                        System.out.println(printMsg);
                        log.info(printMsg);
                        TaskStatics.instance.addConditionNotMatchCnt();
                    }
                } catch (InterruptedException e) {
                    log.error("add task to queue occur a exception", e);
                    throw new IOException(e.getMessage());
                }
            }
            TaskStatics.instance.setListFinished(true);
        } catch (FileNotFoundException e) {
            log.error("fileList path not exist:", e);
            TaskStatics.instance.setListFinished(false);
        } catch (IOException e) {
            log.error("error ocured:", e);
            TaskStatics.instance.setListFinished(false);
        }
    }

    public void buildTask() {
        log.info(config.toString());
        if(config.isFileListMode()) {
            buildFileListTask();
            return;
        }

        SimpleFileVisitor<Path> finder = new SimpleFileVisitor<Path>() {

            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs)
                    throws IOException {
                String dirPath = "";
                try {
                    dirPath = SystemUtils.formatLocalPath(dir.toString());
                } catch (IllegalArgumentException e) {
                    log.error("skip the folder and it's sub member for illegal utf-8 letter");
                    return FileVisitResult.SKIP_SUBTREE;
                }
                if (((CopyFromLocalConfig) config).isExcludes(dirPath)) {
                    log.info("exclude folder: " + dirPath);
                    return FileVisitResult.SKIP_SUBTREE;
                } else {
                    return super.preVisitDirectory(dir, attrs);
                }
            }

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
                    throws IOException {
                String localPath = "";
                try {
                    localPath = SystemUtils.formatLocalPath(file.toString());
                } catch (IllegalArgumentException e) {
                    log.error("skip the file for illegal utf-8 letter");
                    return super.visitFile(file, attrs);
                }
                try {
                    String reason = ((CopyFromLocalConfig) config).needToMigrate(file, localPath);
                    if (reason.isEmpty()) {
                        File localFile = new File(file.toString());

                        MigrateLocalTask migrateLocalTask = new MigrateLocalTask(semaphore,
                                ((CopyFromLocalConfig) config), smallFileTransferManager,
                                bigFileTransferManager, recordDb, localFile);
                        AddTask(migrateLocalTask);
                    } else {
                        String printMsg = String.format(
                                "[condition_not_match] [reason: %s]  [local_file: %s]", reason,
                                file.toString());
                        System.out.println(printMsg);
                        log.info(printMsg);
                        TaskStatics.instance.addConditionNotMatchCnt();
                    }
                } catch (InterruptedException e) {
                    log.error("visit file occur a exception", e);
                    throw new IOException(e.getMessage());
                }
                return super.visitFile(file, attrs);
            }
        };

        log.info("ready to scan folder: " + localFolder);
        try {
            java.nio.file.Files.walkFileTree(Paths.get(localFolder),
                    EnumSet.of(FOLLOW_LINKS), Integer.MAX_VALUE, finder);

            TaskStatics.instance.setListFinished(true);

        } catch (IOException e) {
            TaskStatics.instance.setListFinished(false);
            log.error("walk file tree error", e);
        }
    }

    @Override
    public void waitTaskOver() {
        super.waitTaskOver();
    }
}