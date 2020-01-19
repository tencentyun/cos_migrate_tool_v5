package com.qcloud.cos_migrate_tool.task;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.qcloud.cos_migrate_tool.config.CopyFromUpyunConfig;
import com.qcloud.cos_migrate_tool.config.MigrateType;
import com.qcloud.cos_migrate_tool.meta.TaskStatics;
import com.qcloud.cos_migrate_tool.utils.SystemUtils;

import com.upyun.Result;
import com.upyun.UpException;
import com.UpYun;
import com.UpYun.FolderItemIter;


public class MigrateUpyunTaskExecutor extends TaskExecutor {

    private static final Logger log = LoggerFactory.getLogger(MigrateUpyunTaskExecutor.class);

    private String bucketName;
    private String cosFolder;

    private UpYun upyun;
    private String srcBucket;
    private String srcPrefix;
    private String srcAccessKeyId;
    private String srcAccessKeySecret;
    private String srcEndpoint;

    private CopyFromUpyunConfig config;

    public MigrateUpyunTaskExecutor(CopyFromUpyunConfig config) {
        super(MigrateType.MIGRATE_FROM_UPYUN, config);
        this.bucketName = config.getBucketName();

        this.cosFolder = config.getCosPath();

        this.srcAccessKeyId = config.getSrcAccessKeyId();
        this.srcAccessKeySecret = config.getSrcAccessKeySecret();
        this.srcBucket = config.getSrcBucket();
        this.srcEndpoint = config.getSrcEndpoint();
        this.srcPrefix = config.getSrcPrefix();
        this.config = config;

        this.upyun = new UpYun(this.srcBucket, this.srcAccessKeyId, this.srcAccessKeySecret);
        upyun.setTimeout(60);
        upyun.setApiDomain(UpYun.ED_AUTO);

        if (!config.getSrcProxyHost().isEmpty() && config.getSrcProxyPort() > 0) {
            System.out.println("use proxy");
            System.setProperty("java.net.useSystemProxies", "true");
            System.setProperty("http.proxyHost", config.getSrcProxyHost());
            System.setProperty("http.proxyPort", Integer.toString(config.getSrcProxyPort()));
        }
        
               
    }

    @Override
    public String buildTaskDbComment() {
        String comment = String.format(
                "[time: %s], [srcBucketName: %s] [cosFolder: %s], [srcEndPoint: %s], [srcPrefix: %s]",
                SystemUtils.getCurrentDateTime(), config.getSrcBucket(), cosFolder,
                config.getSrcEndpoint(), config.getSrcPrefix());
        return comment;
    }

    @Override
    public String buildTaskDbFolderPath() {
        String temp = String.format("[srcEndpoint: %s] [prefix: %s] [cosFolder: %s] ", srcEndpoint,
                srcPrefix, cosFolder);
        String dbFolderPath =
                String.format("db/migrate_from_upyun/%s/%s", bucketName, DigestUtils.md5Hex(temp));
        return dbFolderPath;
    }

    public void buildTask() {

        int retry_num = 0;
        LinkedList<String> dirList = new LinkedList<String>();
        LinkedList<String> itrList = new LinkedList<String>();
        if (this.srcPrefix.isEmpty()) {
            dirList.add("/");
        } else {
            dirList.add(this.srcPrefix);
        }
        itrList.add("");

        do {
            String curDir = "";
            String lastItr = "";
            try {
                while (!dirList.isEmpty()) {
                    curDir = dirList.removeFirst();
                    if (itrList.size() > 0) {
                        lastItr = itrList.removeFirst();
                    } else {
                        lastItr = "";
                    }
                    FolderItemIter folderItemIter;
                    do {
                        Map<String, String> params = new HashMap<String, String>();

                        params.put("x-list-iter", lastItr);
                        params.put("x-list-limit", "1000");
                        if (!config.isAscendingOrder()) {
                            params.put("x-list-order", "desc");
                        }

                        folderItemIter = upyun.readDirIter(curDir, params);
                        lastItr = folderItemIter.iter;
                        
                        for (int i = 0; i < folderItemIter.files.size(); ++i) {
                            if (folderItemIter.files.get(i).type.equals("folder")) {

                                dirList.add(curDir + folderItemIter.files.get(i).name + "/");
                            } else {
                                MigrateUpyunTask task = new MigrateUpyunTask(config, null,
                                        curDir + folderItemIter.files.get(i).name,
                                        folderItemIter.files.get(i).size,
                                        folderItemIter.files.get(i).date,
                                        folderItemIter.files.get(i).type, smallFileTransferManager,
                                        bigFileTransferManager, recordDb, semaphore);

                                AddTask(task);
                            }
                        }
                    } while (folderItemIter.files.size() > 0);
                }

                TaskStatics.instance.setListFinished(true);
                return;

            } catch (Exception e) {
                dirList.addFirst(curDir);
                itrList.addFirst(lastItr);
                log.error("curDir:{},lastItr:{},retry_time:{}, Exception:{}", curDir, lastItr,
                        retry_num, e.getMessage());
                TaskStatics.instance.setListFinished(false);
            }

            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }

            retry_num++;
        } while (retry_num < 300);

    }

    @Override
    public void waitTaskOver() {
        super.waitTaskOver();
    }

}
