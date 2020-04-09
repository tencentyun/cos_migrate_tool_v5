package com.qcloud.cos_migrate_tool.task;

import java.util.List;

import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.qcloud.cos_migrate_tool.config.CopyFromAliConfig;
import com.qcloud.cos_migrate_tool.config.MigrateType;
import com.qcloud.cos_migrate_tool.meta.TaskStatics;
import com.qcloud.cos_migrate_tool.utils.SystemUtils;

import com.aliyun.oss.*;
import com.aliyun.oss.common.comm.Protocol;
import com.aliyun.oss.model.ListObjectsRequest;
import com.aliyun.oss.model.OSSObjectSummary;
import com.aliyun.oss.model.ObjectListing;

public class MigrateAliTaskExecutor extends TaskExecutor {

    private static final Logger log = LoggerFactory.getLogger(MigrateLocalTaskExecutor.class);

    private String bucketName;
    private String cosFolder;

    private OSSClient ossClient;
    private String srcBucket;
    private String srcPrefix;
    private String srcAccessKeyId;
    private String srcAccessKeySecret;
    private String srcEndpoint;

    private CopyFromAliConfig config;

    public MigrateAliTaskExecutor(CopyFromAliConfig config) {
        super(MigrateType.MIGRATE_FROM_ALI, config);
        this.bucketName = config.getBucketName();

        this.cosFolder = config.getCosPath();

        this.srcAccessKeyId = config.getSrcAccessKeyId();
        this.srcAccessKeySecret = config.getSrcAccessKeySecret();
        this.srcBucket = config.getSrcBucket();
        this.srcEndpoint = config.getSrcEndpoint();
        this.srcPrefix = config.getSrcPrefix();
        this.config = config;

        ClientConfiguration ossConf = new ClientConfiguration();
        ossConf.setConnectionTimeout(config.getSrcConnectTimeout());
        ossConf.setMaxErrorRetry(20);
        ossConf.setSocketTimeout(config.getSrcSocketTimeout());
        ossConf.setMaxConnections(1024);
        if(config.isEnableSrcHttps()) {
            ossConf.setProtocol(Protocol.HTTPS);
        } else {
            ossConf.setProtocol(Protocol.HTTP);
        }
        if (!config.getSrcProxyHost().isEmpty()) {
            ossConf.setProxyHost(config.getSrcProxyHost());
        }

        if (config.getSrcProxyPort() > 0) {
            ossConf.setProxyPort(config.getSrcProxyPort());
        }

        this.ossClient = new OSSClient(this.srcEndpoint, this.srcAccessKeyId,
                this.srcAccessKeySecret, ossConf);
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
                String.format("db/migrate_from_ali/%s/%s", bucketName, DigestUtils.md5Hex(temp));
        return dbFolderPath;
    }

    public void buildTask() {
        final int maxKeys = 1000;
        final String keyPrefix = this.srcPrefix;
        String nextMarker = "";
        ObjectListing objectListing;

        int retry_num = 0;

        String[] progress = this.recordDb.getListProgress();
        if (config.isResume() && progress != null) {
            nextMarker = progress[1];
        }

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
                                s.getSize(), s.getETag(), s.getLastModified(),
                                smallFileTransferManager, bigFileTransferManager, recordDb,
                                semaphore);

                        AddTask(task);
                    }
                    nextMarker = com.qcloud.cos.utils.UrlEncoderUtils
                            .urlDecode(objectListing.getNextMarker());
                    if (nextMarker != null) {
                        this.recordDb.saveListProgress(keyPrefix, nextMarker);
                    }
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

    }

    @Override
    public void waitTaskOver() {
        super.waitTaskOver();
        this.ossClient.shutdown();
    }

}
