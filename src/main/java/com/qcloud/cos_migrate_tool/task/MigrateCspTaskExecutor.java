package com.qcloud.cos_migrate_tool.task;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;

import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.qcloud.cos.COSClient;
import com.qcloud.cos.ClientConfig;
import com.qcloud.cos.auth.BasicCOSCredentials;
import com.qcloud.cos.auth.COSCredentials;
import com.qcloud.cos.endpoint.SuffixEndpointBuilder;
import com.qcloud.cos.exception.CosServiceException;
import com.qcloud.cos.http.HttpProtocol;
import com.qcloud.cos.model.COSObjectSummary;
import com.qcloud.cos.model.ListObjectsRequest;
import com.qcloud.cos.model.ObjectListing;
import com.qcloud.cos.region.Region;
import com.qcloud.cos_migrate_tool.config.CopyFromCspConfig;
import com.qcloud.cos_migrate_tool.config.MigrateType;
import com.qcloud.cos_migrate_tool.meta.TaskStatics;
import com.qcloud.cos_migrate_tool.utils.SystemUtils;

public class MigrateCspTaskExecutor extends TaskExecutor {
    private static final Logger log = LoggerFactory.getLogger(MigrateCspTaskExecutor.class);

    private COSClient srcCosClient;
    private String cosFolder;
    private String srcBucketName;
    // private String srcCosPath;
    private String srcPrefix;
    private String srcEndPoint;

    public MigrateCspTaskExecutor(CopyFromCspConfig config) {
        super(MigrateType.MIGRATE_FROM_CSP, config);

        COSCredentials srcCred =
                new BasicCOSCredentials(config.getSrcAccessKeyId(), config.getSrcAccessKeySecret());
        ClientConfig clientConfig = new ClientConfig();
        if (config.isEnableHttps()) {
            clientConfig.setHttpProtocol(HttpProtocol.https);
        }

        clientConfig.setRegion(new Region(""));
        clientConfig.setEndpointBuilder(new SuffixEndpointBuilder(config.getSrcEndpoint()));

        clientConfig.setConnectionTimeout(5000);
        clientConfig.setSocketTimeout(5000);

        clientConfig.setUserAgent("cos-migrate-tool-v1.3.6");
        this.srcCosClient = new COSClient(srcCred, clientConfig);
        this.srcEndPoint = config.getSrcEndpoint();
        this.srcBucketName = config.getSrcBucket();
        this.srcPrefix = config.getSrcPrefix();
        this.cosFolder = config.getCosPath();
        this.config = config;
    }

    @Override
    protected String buildTaskDbComment() {
        String comment = String.format(
                "[time: %s], [destRegion: %s], [destBucketName: %s], [destCosFolder: %s], [endpoint: %s], [srcBucketName: %s], [srcPrefix: %s], [smallTaskExecutor: %d]\n",
                SystemUtils.getCurrentDateTime(), config.getRegion(), config.getBucketName(),
                config.getCosPath(), srcEndPoint, srcBucketName, srcPrefix,
                this.smallFileUploadExecutorNum);
        return comment;
    }

    @Override
    protected String buildTaskDbFolderPath() {
        String temp = String.format(
                "[destCosFolder: %s], [endPoint: %s], [srcBucket: %s], [srcPrefix: %s]",
                config.getCosPath(), srcEndPoint, ((CopyFromCspConfig) config).getSrcBucket(),
                ((CopyFromCspConfig) config).getSrcPrefix());
        String dbFolderPath = String.format("db/migrate_copy_csp/%s/%s", config.getBucketName(),
                DigestUtils.md5Hex(temp));
        return dbFolderPath;
    }

    @Override
    public void buildTask() {



        ListObjectsRequest listObjectsRequest =
                new ListObjectsRequest(srcBucketName, srcPrefix, null, null, 1000);

        ObjectListing objectListing;
        int retry_num = 0;

        String urlList = ((CopyFromCspConfig) config).getUrlList();

        if (!urlList.isEmpty()) {
            FileInputStream inputStream = null;
            try {
                inputStream = new FileInputStream(urlList);
            } catch (FileNotFoundException e) {
                e.printStackTrace();
                return;
            }
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));

            String str = null;
            try {
                while ((str = bufferedReader.readLine()) != null) {
                    str = str.trim();
                    System.out.println(str);
                    MigrateCspTask task = new MigrateCspTask((CopyFromCspConfig) config,
                            srcCosClient, str, -1, "", smallFileTransferManager,
                            bigFileTransferManager, recordDb, semaphore);
                    log.info("list from file key: {}", str);
                    try {
                        AddTask(task);
                    } catch (InterruptedException e) {
                        log.error("add task occur a exception:{}", e.toString());
                        e.printStackTrace();
                    }
                    

                }

                inputStream.close();
                bufferedReader.close();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
                return;
            }

        } else {

            do {
                try {
                    while (true) {

                        objectListing = srcCosClient.listObjects(listObjectsRequest);
                        List<COSObjectSummary> cosObjectSummaries =
                                objectListing.getObjectSummaries();

                        for (COSObjectSummary cosObjectSummary : cosObjectSummaries) {
                            String srcKey = cosObjectSummary.getKey();
                            String srcEtag = cosObjectSummary.getETag();
                            long srcSize = cosObjectSummary.getSize();
                            MigrateCspTask task =
                                    new MigrateCspTask((CopyFromCspConfig) config, srcCosClient,
                                            srcKey, srcSize, srcEtag, smallFileTransferManager,
                                            bigFileTransferManager, recordDb, semaphore);
                            log.info("list key: {}, size: {}, etag: {}", srcKey, srcSize, srcEtag);

                            AddTask(task);

                        }

                        if (!objectListing.isTruncated()) {
                            break;
                        }

                        listObjectsRequest.setMarker(objectListing.getNextMarker());
                    }

                    TaskStatics.instance.setListFinished(true);

                    return;

                } catch (CosServiceException e) {
                    log.error("List cos bucket occur a exception:{}", e.toString());
                    TaskStatics.instance.setListFinished(false);
                    if (e.getStatusCode() == 503) {
                        try {
                            Thread.sleep(1 * 100);
                        } catch (InterruptedException e1) {
                            e1.printStackTrace();
                        }
                    }
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
