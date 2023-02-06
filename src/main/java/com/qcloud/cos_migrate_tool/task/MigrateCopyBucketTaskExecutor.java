package com.qcloud.cos_migrate_tool.task;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;

import com.qcloud.cos.COSClient;
import com.qcloud.cos.ClientConfig;
import com.qcloud.cos.auth.BasicCOSCredentials;
import com.qcloud.cos.auth.BasicSessionCredentials;
import com.qcloud.cos.auth.COSCredentials;
import com.qcloud.cos.http.HttpProtocol;
import com.qcloud.cos.model.COSObjectSummary;
import com.qcloud.cos.model.ListObjectsRequest;
import com.qcloud.cos.model.ObjectListing;
import com.qcloud.cos.model.StorageClass;
import com.qcloud.cos.region.Region;
import com.qcloud.cos.utils.UrlEncoderUtils;
import com.qcloud.cos_migrate_tool.config.CopyBucketConfig;
import com.qcloud.cos_migrate_tool.config.MigrateType;
import com.qcloud.cos_migrate_tool.meta.TaskStatics;
import com.qcloud.cos_migrate_tool.utils.SystemUtils;
import com.qcloud.cos_migrate_tool.utils.VersionInfoUtils;

import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MigrateCopyBucketTaskExecutor extends TaskExecutor {
    private static final Logger log = LoggerFactory.getLogger(MigrateCopyBucketTaskExecutor.class);

    private COSClient srcCosClient;
    private String srcRegion;
    private String srcBucketName;
    private String srcCosPath;
    private String srcFileList;

    public MigrateCopyBucketTaskExecutor(CopyBucketConfig config) {
        super(MigrateType.MIGRATE_FROM_COS_BUCKET_COPY, config);
        String src_token = ((CopyBucketConfig) config).getSrcToken();
        COSCredentials srcCred = null;
        if (src_token != null && !src_token.isEmpty()) {
            log.info("Use temporary token to list object");
            System.out.println("Use temporary token to list object");
            srcCred = new BasicSessionCredentials(config.getSrcAk(), config.getSrcSk(), src_token);
        } else {
            srcCred = new BasicCOSCredentials(config.getSrcAk(), config.getSrcSk());
        }
        ClientConfig clientConfig = new ClientConfig(new Region(config.getSrcRegion()));
        if (config.isEnableHttps()) {
            clientConfig.setHttpProtocol(HttpProtocol.https);
        } else {
            clientConfig.setHttpProtocol(HttpProtocol.http);
        }

        // 源也是cos的，就直接使用一样的配置，没有再像从其他云一样使用单独的src配置
        if (config.isShortConnection()) {
            clientConfig.setShortConnection();
        }

        if (config.getSrcEndpointSuffix() != null) {
            clientConfig.setEndPointSuffix(config.getSrcEndpointSuffix());
        }
        clientConfig.setConnectionTimeout(5000);
        clientConfig.setSocketTimeout(5000);

        clientConfig.setUserAgent(VersionInfoUtils.getUserAgent());
        this.srcCosClient = new COSClient(srcCred, clientConfig);
        this.srcRegion = config.getSrcRegion();
        this.srcBucketName = config.getSrcBucket();
        this.srcCosPath = config.getSrcCosPath();
        this.srcFileList = config.getSrcFileList();
    }

    @Override
    protected String buildTaskDbComment() {
        String comment = String.format(
                "[time: %s], [destRegion: %s], [destBucketName: %s], [destCosFolder: %s], [srcRegion: %s], [srcFileList:%s], [srcBucketName: %s], [srcFolder: %s], [smallTaskExecutor: %d]\n",
                SystemUtils.getCurrentDateTime(), config.getRegion(), config.getBucketName(),
                config.getCosPath(), srcRegion, srcFileList, srcBucketName, srcCosPath,
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

        int lastDelimiter = srcCosPath.lastIndexOf("/");

        if (!srcFileList.isEmpty()) {
            File file = new File(srcFileList);
            if (!file.isFile() || !file.exists()) {
                String printMsg = String.format("file[%s] not exist or not file", srcFileList);
                log.error(printMsg);
                System.out.println(printMsg);
            }

            InputStreamReader read = null;
            try {
                read = new InputStreamReader(new FileInputStream(file));
            } catch (FileNotFoundException e1) {
                e1.printStackTrace();
                return;
            }

            BufferedReader bufferedReader = new BufferedReader(read);
            String srcKey = null;

            try {
                while ((srcKey = bufferedReader.readLine()) != null) {

                    srcKey = UrlEncoderUtils.urlDecode(srcKey);

                    String copyDestKey = null;
                    if (srcKey.startsWith("/")) {
                        copyDestKey = config.getCosPath() + srcKey.substring(1);
                        srcKey = srcKey.substring(1);
                    } else {
                        copyDestKey = config.getCosPath() + srcKey;
                    }

                    // set storage class to Standard just for a non-null value needed
                    // no side effect
                    MigrateCopyBucketTask task =
                            new MigrateCopyBucketTask(semaphore, (CopyBucketConfig) config,
                                    smallFileTransferManager, bigFileTransferManager, recordDb,
                                    srcCosClient, srcKey, 0, "", StorageClass.Standard, copyDestKey);

                    AddTask(task);

                }

                TaskStatics.instance.setListFinished(true);

            } catch (IOException e) {
                log.error(e.toString());
                TaskStatics.instance.setListFinished(false);
                e.printStackTrace();
            } catch (Exception e) {
                log.error(e.toString());
                e.printStackTrace();
                TaskStatics.instance.setListFinished(false);
            }

            try {
                bufferedReader.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            try {
                read.close();
            } catch (IOException e) {
                e.printStackTrace();
            }


        } else {
            String nextMarker = "";
            String[] progress = this.recordDb.getListProgress();
            if (config.isResume() && progress != null) {
                nextMarker = progress[1];
            }

            ListObjectsRequest listObjectsRequest =
                    new ListObjectsRequest(srcBucketName, srcCosPath, nextMarker, null, 1000);

            ObjectListing objectListing;
            int retry_num = 0;

            do {
                try {
                    while (true) {
                        listObjectsRequest.setMarker(nextMarker);
                        objectListing = srcCosClient.listObjects(listObjectsRequest);
                        List<COSObjectSummary> cosObjectSummaries =
                                objectListing.getObjectSummaries();

                        for (COSObjectSummary cosObjectSummary : cosObjectSummaries) {
                            String srcKey = cosObjectSummary.getKey();
                            String srcEtag = cosObjectSummary.getETag();
                            long srcSize = cosObjectSummary.getSize();
                            String keyName = srcKey.substring(lastDelimiter);
                            String copyDestKey = config.getCosPath() + keyName;

                            MigrateCopyBucketTask task = new MigrateCopyBucketTask(semaphore,
                                    (CopyBucketConfig) config, smallFileTransferManager,
                                    bigFileTransferManager, recordDb, srcCosClient, srcKey, srcSize,
                                    srcEtag, StorageClass.fromValue(cosObjectSummary.getStorageClass()),
                                    copyDestKey);

                            AddTask(task);
                        }
                        nextMarker = objectListing.getNextMarker();
                        if (nextMarker != null) {
                            this.recordDb.saveListProgress(srcCosPath, nextMarker);
                        }
                        if (!objectListing.isTruncated()) {
                            break;
                        }
                    }

                    TaskStatics.instance.setListFinished(true);

                    return;

                } catch (Exception e) {
                    log.error("List cos bucket occur a exception:{}", e.toString());
                    TaskStatics.instance.setListFinished(false);
                }

                ++retry_num;

            } while (retry_num < 20);
        }

    }

    @Override
    public void waitTaskOver() {
        super.waitTaskOver();
        srcCosClient.shutdown();
    }

}
