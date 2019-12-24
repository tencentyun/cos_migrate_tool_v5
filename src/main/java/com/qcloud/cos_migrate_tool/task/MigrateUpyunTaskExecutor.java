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

        try {
            LinkedList<String> dirList = new LinkedList<String>();
            dirList.add("");
            while (!dirList.isEmpty()) {
                String curDir = dirList.removeFirst();
                String lastItr = "";

                FolderItemIter folderItemIter;
                do {
                    Map<String, String> params = new HashMap<String, String>();

                    params.put("x-list-iter", lastItr);
                    params.put("x-list-limit", "1000");

                    folderItemIter = upyun.readDirIter(curDir, params);
                    lastItr = folderItemIter.iter;
                    for (int i = 0; i < folderItemIter.files.size(); ++i) {
                        if (folderItemIter.files.get(i).type.equals("folder")) {

                            dirList.add(curDir + "/" + folderItemIter.files.get(i).name);
                        } else {
                            MigrateUpyunTask task = new MigrateUpyunTask(config, upyun,
                                    curDir + "/" + folderItemIter.files.get(i).name,
                                    folderItemIter.files.get(i).size,
                                    folderItemIter.files.get(i).date, folderItemIter.files.get(i).type, smallFileTransferManager,
                                    bigFileTransferManager, recordDb, semaphore);

                            AddTask(task);
                        }
                    }
                } while (folderItemIter.files.size() > 0);
            }

            TaskStatics.instance.setListFinished(true);

        } catch (Exception e) {
            log.error(e.getMessage());
            TaskStatics.instance.setListFinished(false);
        }



        /*
        final int maxKeys = 200;
        final String keyPrefix = this.srcPrefix;
        String nextMarker = "";
        ObjectListing objectListing;
        
        int retry_num = 0;
        
        do {
            try {
                do {
                    objectListing = ossClient.listObjects(new ListObjectsRequest(this.srcBucket)
                            .withPrefix(keyPrefix).withMarker(nextMarker).withMaxKeys(maxKeys)
                            .withEncodingType("url"));
                    log.info("list next marker: " + nextMarker);
                    List<OSSObjectSummary> sums = objectListing.getObjectSummaries();
                    for (OSSObjectSummary s : sums) {
                        // AddTask
                        MigrateAliTask task = new MigrateAliTask(config, ossClient,
                                com.qcloud.cos.utils.UrlEncoderUtils.urlDecode(s.getKey()),
                                s.getSize(), s.getETag(), s.getLastModified(), smallFileTransferManager,
                                bigFileTransferManager, recordDb, semaphore);
        
                            AddTask(task);
                    }
                    nextMarker = com.qcloud.cos.utils.UrlEncoderUtils
                            .urlDecode(objectListing.getNextMarker());
                } while (objectListing.isTruncated());
        
                TaskStatics.instance.setListFinished(true);
                return;
        
            } catch (OSSException e) {
                log.error("list fail msg: {}", e.getMessage());
                TaskStatics.instance.setListFinished(false);
                if (e.getErrorCode().equalsIgnoreCase("AccessDenied")) {
                    System.out.println(e.getMessage());
                    break;
                }
            } catch (ClientException e) {
                log.error("list fail msg: {}", e.getMessage());
                TaskStatics.instance.setListFinished(false);
                if (e.getErrorCode().equalsIgnoreCase("AccessDenied")) {
                    System.out.println(e.getMessage());
                    break;
                }
            } catch (Exception e) {
                log.error(e.getMessage());
                TaskStatics.instance.setListFinished(false);
            }
            retry_num++;
        } while (retry_num < 20);
        
        */

    }

    @Override
    public void waitTaskOver() {
        super.waitTaskOver();
    }

}
