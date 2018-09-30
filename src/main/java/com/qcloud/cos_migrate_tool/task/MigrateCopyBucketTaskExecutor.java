package com.qcloud.cos_migrate_tool.task;

import java.util.List;

import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.qcloud.cos.COSClient;
import com.qcloud.cos.ClientConfig;
import com.qcloud.cos.auth.BasicCOSCredentials;
import com.qcloud.cos.auth.COSCredentials;
import com.qcloud.cos.http.HttpProtocol;
import com.qcloud.cos.model.COSObjectSummary;
import com.qcloud.cos.model.ListObjectsRequest;
import com.qcloud.cos.model.ObjectListing;
import com.qcloud.cos.region.Region;
import com.qcloud.cos_migrate_tool.config.CopyBucketConfig;
import com.qcloud.cos_migrate_tool.config.MigrateType;
import com.qcloud.cos_migrate_tool.meta.TaskStatics;
import com.qcloud.cos_migrate_tool.utils.SystemUtils;

public class MigrateCopyBucketTaskExecutor extends TaskExecutor {
    private static final Logger log = LoggerFactory.getLogger(MigrateCopyBucketTaskExecutor.class);

    private COSClient srcCosClient;
    private String srcRegion;
    private String srcBucketName;
    private String srcCosPath;

    public MigrateCopyBucketTaskExecutor(CopyBucketConfig config) {
        super(MigrateType.MIGRATE_FROM_COS_BUCKET_COPY, config);

        COSCredentials srcCred = new BasicCOSCredentials(config.getSrcAk(), config.getSrcSk());
        ClientConfig clientConfig = new ClientConfig(new Region(config.getSrcRegion()));
        if (config.isEnableHttps()) {
            clientConfig.setHttpProtocol(HttpProtocol.https);
        }
        if (config.getSrcEndpointSuffix() != null) {
            clientConfig.setEndPointSuffix(config.getSrcEndpointSuffix());
        }

        clientConfig.setUserAgent("cos-migrate-tool-v1.0");
        this.srcCosClient = new COSClient(srcCred, clientConfig);
        this.srcRegion = config.getSrcRegion();
        this.srcBucketName = config.getSrcBucket();
        this.srcCosPath = config.getSrcCosPath();
    }

    @Override
    protected String buildTaskDbComment() {
        String comment = String.format(
                "[time: %s], [destRegion: %s], [destBucketName: %s], [destCosFolder: %s], [srcRegion: %s], [srcBucketName: %s], [srcFolder: %s], [smallTaskExecutor: %d]\n",
                SystemUtils.getCurrentDateTime(), config.getRegion(), config.getBucketName(),
                config.getCosPath(), srcRegion, srcBucketName, srcCosPath,
                this.smallFileUploadExecutorNum);
        return comment;
    }

    @Override
    protected String buildTaskDbFolderPath() {
        String temp = String.format(
                "[destCosFolder: %s], [srcRegion: %s], [srcBucket: %s], [srcCosFolder: %s]",
                config.getCosPath(), ((CopyBucketConfig) config).getSrcRegion(),
                ((CopyBucketConfig) config).getSrcBucket(),
                ((CopyBucketConfig) config).getSrcCosPath());
        String dbFolderPath = String.format("db/migrate_copy_bucket/%s/%s", config.getBucketName(),
                DigestUtils.md5Hex(temp));
        return dbFolderPath;
    }

    @Override
    public void buildTask() {
        ListObjectsRequest listObjectsRequest =
                new ListObjectsRequest(srcBucketName, srcCosPath, null, null, 1000);

        int lastDelimiter = srcCosPath.lastIndexOf("/");
        ObjectListing objectListing;
        int retry_num = 0;

        do {
            try {
                while (true) {
                    objectListing = srcCosClient.listObjects(listObjectsRequest);
                    List<COSObjectSummary> cosObjectSummaries = objectListing.getObjectSummaries();
                    for (COSObjectSummary cosObjectSummary : cosObjectSummaries) {
                        String srcKey = cosObjectSummary.getKey();
                        String srcEtag = cosObjectSummary.getETag();
                        long srcSize = cosObjectSummary.getSize();
                        String keyName = srcKey.substring(lastDelimiter);
                        String copyDestKey = config.getCosPath() + keyName;

                        MigrateCopyBucketTask task =
                                new MigrateCopyBucketTask(semaphore, (CopyBucketConfig) config,
                                        smallFileTransferManager, bigFileTransferManager, recordDb,
                                        srcCosClient, srcKey, srcSize, srcEtag, copyDestKey);
                        AddTask(task);
                    }
                    if (!objectListing.isTruncated()) {
                        break;
                    }
                    listObjectsRequest.setMarker(objectListing.getNextMarker());
                }

                TaskStatics.instance.setListFinished(true);
                
                return;

            } catch (Exception e) {
                log.error("List cos bucket occur a exception", e);
                TaskStatics.instance.setListFinished(false);
            }
            
            ++retry_num;
            
        } while (retry_num < 5);

    }

    @Override
    public void waitTaskOver() {
        super.waitTaskOver();
        srcCosClient.shutdown();
    }

}
