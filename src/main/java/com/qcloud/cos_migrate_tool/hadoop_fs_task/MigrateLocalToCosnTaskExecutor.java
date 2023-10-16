package com.qcloud.cos_migrate_tool.hadoop_fs_task;

import com.qcloud.cos_migrate_tool.config.CopyFromLocalToCosnConfig;
import com.qcloud.cos_migrate_tool.config.MigrateType;
import com.qcloud.cos_migrate_tool.meta.TaskStatics;
import com.qcloud.cos_migrate_tool.task.MigrateLocalTaskExecutor;
import com.qcloud.cos_migrate_tool.task.TaskExecutor;
import com.qcloud.cos_migrate_tool.utils.SystemUtils;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URI;
import java.nio.file.FileVisitResult;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.EnumSet;

import static java.nio.file.FileVisitOption.FOLLOW_LINKS;

public class MigrateLocalToCosnTaskExecutor extends TaskExecutor {
    private static final Logger log = LoggerFactory.getLogger(MigrateLocalTaskExecutor.class);
    private FileSystem fs;
    private String bucketName;
    private String localFolder;
    private String cosFolder;
    private CopyFromLocalToCosnConfig config;

    public MigrateLocalToCosnTaskExecutor(CopyFromLocalToCosnConfig config) {
        super(MigrateType.MIGRATE_FROM_LOCAL_TO_COSN_FS, config);
        this.bucketName = config.getBucketName();
        this.localFolder = config.getLocalPath();
        this.cosFolder = config.getCosPath();
        this.config = config;
        this.fs = initCosnFs();
    }

    @Override
    public String buildTaskDbComment() {
        String comment = String.format(
                "[upload by cosn fs],[time: %s], [bucketName: %s], [localFolder: %s], [cosFolder: %s], [smallfile_exector_number: %d], [bigfile_executor_number: %d]\n",
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

    private void buildFileListTask() {
        String localPathPrefix = "";
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

                        MigrateLocalToCosnFsTask migrateLocalToCosnTask = new MigrateLocalToCosnFsTask(semaphore,
                                (CopyFromLocalToCosnConfig) config, smallFileTransferManager, bigFileTransferManager, recordDb, localFile,fs);
                        AddTask(migrateLocalToCosnTask);
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
                if (((CopyFromLocalToCosnConfig) config).isExcludes(dirPath)) {
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
                    String reason = ((CopyFromLocalToCosnConfig) config).needToMigrate(file, localPath);
                    if (reason.isEmpty()) {
                        File localFile = new File(file.toString());

                        MigrateLocalToCosnFsTask migrateLocalToCosnTask = new MigrateLocalToCosnFsTask(semaphore,
                                ((CopyFromLocalToCosnConfig) config), smallFileTransferManager,
                                bigFileTransferManager, recordDb, localFile,fs);
                        AddTask(migrateLocalToCosnTask);
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

    public FileSystem initCosnFs() {
        Configuration conf = new Configuration();

        String[] parts = config.getBucketName().split("-");
        String appid = parts[parts.length - 1];

        conf.set("fs.cosn.impl", "org.apache.hadoop.fs.CosFileSystem");
        conf.set("fs.AbstractFileSystem.cosn.impl", "org.apache.hadoop.fs.CosN");
        conf.set("fs.cosn.userinfo.secretId",  config.getAk());
        conf.set("fs.cosn.userinfo.secretKey", config.getSk());
        conf.set("fs.cosn.bucket.region", config.getRegion());
        conf.set("fs.cosn.tmp.dir", config.getTempFolderPath());
        conf.set("fs.cosn.trsf.fs.ofs.tmp.cache.dir", config.getTempFolderPath());
        conf.set("fs.cosn.userinfo.appid", appid);
        String cosnUrl = "cosn://" + config.getBucketName()+"/";


        try {
            fs = FileSystem.get(URI.create(cosnUrl), conf);
        } catch (IOException e) {
            log.error("create cosn fs failed!", e);
            System.out.println("create cosn fs failed: "+e.getMessage());
            return null;
        }
        return fs;
    }

    @Override
    public void waitTaskOver() {
        super.waitTaskOver();
        try {
            this.fs.close();
        } catch (IOException e) {
            log.error("close fs occurred ioexception!", e);
            System.err.println("close fs occurred ioexception!");
        }
    }
}

