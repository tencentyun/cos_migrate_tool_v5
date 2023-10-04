package com.qcloud.cos_migrate_tool.task;

import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.qcloud.cos_migrate_tool.config.CopyFromAwsConfig;
import com.qcloud.cos_migrate_tool.config.MigrateType;
import com.qcloud.cos_migrate_tool.meta.TaskStatics;
import com.qcloud.cos_migrate_tool.utils.SystemUtils;
import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;

public class MigrateAwsTaskExecutor extends TaskExecutor {

    private static final Logger log = LoggerFactory.getLogger(MigrateLocalTaskExecutor.class);


    private String bucketName;
    private String cosFolder;

    private AmazonS3 s3Client;
    private String srcBucket;
    private String srcPrefix;
    private String srcAccessKeyId;
    private String srcAccessKeySecret;
    private String srcEndpoint;

    private CopyFromAwsConfig config;

    public MigrateAwsTaskExecutor(CopyFromAwsConfig config) {
        super(MigrateType.MIGRATE_FROM_AWS, config);
        this.bucketName = config.getBucketName();
        this.cosFolder = config.getCosPath();

        this.srcAccessKeyId = config.getSrcAccessKeyId();
        this.srcAccessKeySecret = config.getSrcAccessKeySecret();
        this.srcBucket = config.getSrcBucket();
        this.srcEndpoint = config.getSrcEndpoint();
        this.srcPrefix = config.getSrcPrefix();
        this.config = config;

        com.amazonaws.ClientConfiguration awsConf = new com.amazonaws.ClientConfiguration();
        awsConf.setConnectionTimeout(config.getSrcConnectTimeout());
        awsConf.setMaxErrorRetry(5);
        awsConf.setSocketTimeout(config.getSrcSocketTimeout());
        awsConf.setMaxConnections(1024);
        if(config.isEnableSrcHttps()) {
            awsConf.setProtocol(Protocol.HTTPS);
        } else {
            awsConf.setProtocol(Protocol.HTTP);
        }

        if (!config.getSrcProxyHost().isEmpty()) {
            awsConf.setProxyHost(config.getSrcProxyHost());
        }

        if (config.getSrcProxyPort() > 0) {
            awsConf.setProxyPort(config.getSrcProxyPort());
        }

        AWSCredentials credentials = new BasicAWSCredentials(srcAccessKeyId, srcAccessKeySecret);
        this.s3Client = AmazonS3ClientBuilder.standard().withClientConfiguration(awsConf)
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .withEndpointConfiguration(new EndpointConfiguration(srcEndpoint, null)).build();

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
        String temp = String.format("[srcPrefix: %s], [cosFolder: %s]", srcPrefix, cosFolder);
        String dbFolderPath =
                String.format("db/migrate_from_aws/%s/%s", bucketName, DigestUtils.md5Hex(temp));
        return dbFolderPath;
    }

    public void buildTask() {
        String nextMarker = "";
        String[] progress = this.recordDb.getListProgress();
        if (config.isResume() && progress != null) {
            nextMarker = progress[1];
        }
        try {
            ListObjectsRequest listObjectsRequest = new ListObjectsRequest();
            listObjectsRequest.setBucketName(srcBucket);
            listObjectsRequest.setPrefix(srcPrefix);
            listObjectsRequest.setMarker(nextMarker);
            ObjectListing objectListing = null;
            do {

                objectListing = s3Client.listObjects(listObjectsRequest);
                for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
                    // AddTask
                    MigrateAwsTask task = new MigrateAwsTask(config, s3Client,
                            objectSummary.getKey(), objectSummary.getSize(),
                            objectSummary.getETag(), smallFileTransferManager,
                            bigFileTransferManager, recordDb, semaphore,fs);
                    log.info("list key: {}, size: {}, etag: {}", objectSummary.getKey(),
                            objectSummary.getSize(), objectSummary.getETag());

                    AddTask(task);
                }
                nextMarker = objectListing.getNextMarker();
                listObjectsRequest.setMarker(nextMarker);
                if (nextMarker != null) {
                    this.recordDb.saveListProgress(srcPrefix, nextMarker);
                }
            } while (objectListing.isTruncated());

            TaskStatics.instance.setListFinished(true);

        } catch (AmazonServiceException ase) {
            log.error("list fail AmazonServiceException errorcode: {}, msg: {}", ase.getErrorCode(),
                    ase.getMessage());
            TaskStatics.instance.setListFinished(false);
        } catch (AmazonClientException ace) {
            log.error("list fail AmazonClientException msg: {}", ace.getMessage().toString());
            TaskStatics.instance.setListFinished(false);
        } catch (Exception e) {
            log.error(e.getMessage());
            TaskStatics.instance.setListFinished(false);
        }

    }

    @Override
    public void waitTaskOver() {
        super.waitTaskOver();
        this.s3Client.shutdown();
    }
}
