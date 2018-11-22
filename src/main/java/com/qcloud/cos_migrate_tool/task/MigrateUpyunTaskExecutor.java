package com.qcloud.cos_migrate_tool.task;


import java.util.List;
import java.util.Iterator;

import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.qcloud.cos_migrate_tool.config.CopyFromUpyunConfig;
import com.qcloud.cos_migrate_tool.config.MigrateType;
import com.qcloud.cos_migrate_tool.meta.TaskStatics;
import com.qcloud.cos_migrate_tool.utils.SystemUtils;

import com.qcloud.cos_migrate_tool.thirdparty.upyun.UpYun;

public class MigrateUpyunTaskExecutor extends TaskExecutor {

    private static final Logger log = LoggerFactory.getLogger(MigrateLocalTaskExecutor.class);

    private String bucketName;
    private String cosFolder;

    private String srcBucket;
    private String srcPrefix;
    private String srcAccessKeyId;
    private String srcAccessKeySecret;
    private String srcEndpoint;

    private CopyFromUpyunConfig config;

    private UpYun upyun;

    public MigrateUpyunTaskExecutor(CopyFromUpyunConfig config) {
        super(MigrateType.MIGRATE_FROM_UPYUN, config);
        this.bucketName = config.getBucketName();
        this.cosFolder = config.getCosPath();

        this.srcAccessKeyId = config.getSrcAccessKeyId();
        this.srcAccessKeySecret = config.getSrcAccessKeySecret();
        this.srcBucket = config.getSrcBucket();
        this.srcEndpoint = config.getSrcEndpoint();
        this.srcPrefix = config.getSrcPrefix();
        if (!this.srcPrefix.endsWith("/")) {
            this.srcPrefix += "/";
        }

        this.config = config;

        this.upyun = new UpYun(this.srcBucket, this.srcAccessKeyId, this.srcAccessKeySecret);
         if (!config.getSrcProxyHost().isEmpty() && config.getSrcProxyPort() > 0) {
            this.upyun.setProxy(config.getSrcProxyHost(), config.getSrcProxyPort());;
        }
    
    }

    @Override
    public String buildTaskDbComment() {
        String comment = String.format(
                "[time: %s], [srcBucketName: %s] [cosFolder: %s], [srcEndPoint: %s], [srcPrefix: %s]",
                SystemUtils.getCurrentDateTime(), config.getSrcBucket(), cosFolder, config.getSrcEndpoint(),
                config.getSrcPrefix());
        return comment;
    }

    @Override
    public String buildTaskDbFolderPath() {
        String temp = String.format("[srcPrefix: %s], [cosFolder: %s]", srcPrefix, cosFolder);
        String dbFolderPath = String.format("db/migrate_from_upyun/%s/%s", bucketName, DigestUtils.md5Hex(temp));
        return dbFolderPath;
    }

    private void buildTaskFor(String path, int level) {
        final int batch_size = 100;
        try {
            UpYun.ReadDirResult rr = upyun.readDir(path, null, batch_size);
            while (rr.items.size() > 0) {
                for(Iterator<UpYun.FolderItem> i = rr.items.iterator(); i.hasNext(); ) {
                    UpYun.FolderItem item = i.next();
                    String itemPath = path + item.name;
                    if (item.type == "Folder") {
                        buildTaskFor(itemPath + "/", level + 1);
                    } else {
                        UpYun.FileInfo info = upyun.getFileInfo(itemPath);
                        MigrateUpyunTask task = new MigrateUpyunTask(config, itemPath, info.size, info.md5, 
                            smallFileTransferManager, bigFileTransferManager, recordDb, semaphore);
                        
                        try {
                            AddTask(task);
                        } catch (InterruptedException e) {
                            log.error(e.getMessage());
                        }                    }
                }
                rr = upyun.readDir(path, rr.nextIter, batch_size);
            }
            if (level == 0) {
                TaskStatics.instance.setListFinished(true);
            }
        } catch (Exception e) {
            log.error(e.getMessage());
            TaskStatics.instance.setListFinished(false);
        }
    }

    public void buildTask() {
        buildTaskFor(this.srcPrefix, 0);
    }
}
