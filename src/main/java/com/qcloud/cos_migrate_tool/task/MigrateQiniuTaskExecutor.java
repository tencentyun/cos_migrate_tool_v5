package com.qcloud.cos_migrate_tool.task;

import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.qcloud.cos_migrate_tool.config.CopyFromQiniuConfig;
import com.qcloud.cos_migrate_tool.config.MigrateType;
import com.qcloud.cos_migrate_tool.meta.TaskStatics;
import com.qcloud.cos_migrate_tool.utils.SystemUtils;
import com.qiniu.common.Zone;
import com.qiniu.http.ProxyConfiguration;
import com.qiniu.storage.BucketManager;
import com.qiniu.storage.Configuration;
import com.qiniu.storage.model.FileInfo;
import com.qiniu.util.Auth;

public class MigrateQiniuTaskExecutor extends TaskExecutor {

    private static final Logger log = LoggerFactory.getLogger(MigrateLocalTaskExecutor.class);

    private String bucketName;
    private String cosFolder;

    private BucketManager bucketManager;
    private Auth auth;
    private String srcBucket;
    private String srcPrefix;
    private String srcAccessKeyId;
    private String srcAccessKeySecret;

    private CopyFromQiniuConfig config;

    public MigrateQiniuTaskExecutor(CopyFromQiniuConfig config) {
        super(MigrateType.MIGRATE_FROM_QINIU, config);
        this.bucketName = config.getBucketName();

        this.cosFolder = config.getCosPath();

        this.srcAccessKeyId = config.getSrcAccessKeyId();
        this.srcAccessKeySecret = config.getSrcAccessKeySecret();
        this.srcBucket = config.getSrcBucket();
        this.srcPrefix = config.getSrcPrefix();
        this.config = config;

        Configuration cfg = new Configuration();
        if (!config.getSrcProxyHost().isEmpty() && config.getSrcProxyPort() > 0) {
            cfg.proxy = new ProxyConfiguration(config.getSrcProxyHost(), config.getSrcProxyPort());
        }
        cfg.useHttpsDomains = false;

        cfg.zone = Zone.autoZone(); // 七牛使用autozone后，list性能不太好，不过一次获取1000，平摊起来耗能接受
        auth = Auth.create(srcAccessKeyId, srcAccessKeySecret);

        bucketManager = new BucketManager(auth, cfg);
    }

    @Override
    public String buildTaskDbComment() {
        String comment = String.format(
                "[time: %s], [srcBucketName: %s] [cosFolder: %s], [endPoint: %s], [srcPrefix: %s]",
                SystemUtils.getCurrentDateTime(), config.getSrcBucket(), cosFolder,
                config.getSrcEndpoint(), config.getSrcPrefix());
        return comment;
    }

    @Override
    public String buildTaskDbFolderPath() {
        String temp = String.format("[srcPrefix: %s] [srcBucket: %s] [cosFolder: %s]", srcPrefix,
                srcBucket, cosFolder);
        String dbFolderPath =
                String.format("db/migrate_from_qiniu/%s/%s", bucketName, DigestUtils.md5Hex(temp));
        return dbFolderPath;
    }

    public void buildTask() {

        // 每次迭代的长度限制，最大1000，推荐值 1000
        int limit = 1000;
        String delimiter = "";
        try {
            BucketManager.FileListIterator fileListIterator =
                    bucketManager.createFileListIterator(srcBucket, srcPrefix, limit, delimiter);
            while (fileListIterator.hasNext()) {
                // 处理获取的file list结果
                FileInfo[] items = fileListIterator.next();
                for (FileInfo item : items) {
                    MigrateQiniuTask task = new MigrateQiniuTask(config, auth, item.key, item.fsize,
                            item.hash, smallFileTransferManager, bigFileTransferManager, recordDb,
                            semaphore);

                    try {
                        AddTask(task);
                    } catch (InterruptedException e) {
                        log.error("add task fail,msg:{}", e.getMessage());
                    }
                }
            }

            TaskStatics.instance.setListFinished(true);
            return;

        } catch (Exception e) {
            log.error("list fail msg:{}", e.getMessage());
            TaskStatics.instance.setListFinished(false);
        }


    }

    @Override
    public void waitTaskOver() {
        super.waitTaskOver();
    }

}
