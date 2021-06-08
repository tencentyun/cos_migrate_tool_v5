package com.qcloud.cos_migrate_tool.task;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;

import com.qcloud.cos_migrate_tool.config.CopyFromCspConfig;
import com.qcloud.cos_migrate_tool.config.MigrateType;
import com.qcloud.cos_migrate_tool.meta.TaskStatics;
import com.qcloud.cos_migrate_tool.utils.SystemUtils;

import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MigrateCspTaskExecutor extends TaskExecutor {
    private static final Logger log = LoggerFactory.getLogger(MigrateCspTaskExecutor.class);

    private com.branch.cos.COSClient srcCosClient;
    private String cosFolder;
    private String srcBucketName;
    // private String srcCosPath;
    private String srcPrefix;
    private String srcEndPoint;

    public MigrateCspTaskExecutor(CopyFromCspConfig config) {
        super(MigrateType.MIGRATE_FROM_CSP, config);

        com.branch.cos.auth.COSCredentials srcCred =
                new com.branch.cos.auth.BasicCOSCredentials(config.getSrcAccessKeyId(), config.getSrcAccessKeySecret());
        com.branch.cos.ClientConfig clientConfig = new com.branch.cos.ClientConfig();
        if (config.isEnableSrcHttps()) {
            clientConfig.setHttpProtocol(com.branch.cos.http.HttpProtocol.https);
        } else {
            clientConfig.setHttpProtocol(com.branch.cos.http.HttpProtocol.http);
        }

        clientConfig.setRegion(new com.branch.cos.region.Region(""));
        clientConfig.setEndpointBuilder(new com.branch.cos.endpoint.SuffixEndpointBuilder(config.getSrcEndpoint()));

        clientConfig.setConnectionTimeout(config.getSrcConnectTimeout());
        clientConfig.setSocketTimeout(config.getSrcSocketTimeout());
        clientConfig.setUserAgent("cos-migrate-tool-v1.3.6");
        clientConfig.setEndpointResolver(new IDCL5EndpointResolver(config.getModId(), config.getCmdId()));
        this.srcCosClient = new com.branch.cos.COSClient(srcCred, clientConfig);
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



        com.branch.cos.model.ListObjectsRequest listObjectsRequest =
                new com.branch.cos.model.ListObjectsRequest(srcBucketName, srcPrefix, null, null, 1000);

        com.branch.cos.model.ObjectListing objectListing;
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
                
                TaskStatics.instance.setListFinished(true);
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
                        List<com.branch.cos.model.COSObjectSummary> cosObjectSummaries =
                                objectListing.getObjectSummaries();

                        for (com.branch.cos.model.COSObjectSummary cosObjectSummary : cosObjectSummaries) {
                            String srcKey = this.srcPrefix + cosObjectSummary.getKey();
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

                } catch (com.branch.cos.exception.CosServiceException e) {
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
