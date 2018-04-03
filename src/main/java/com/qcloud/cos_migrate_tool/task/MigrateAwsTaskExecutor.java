package com.qcloud.cos_migrate_tool.task;

import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.qcloud.cos_migrate_tool.config.CopyFromAwsConfig;
import com.qcloud.cos_migrate_tool.config.MigrateType;
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
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
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
        awsConf.setConnectionTimeout(5000);
        awsConf.setMaxErrorRetry(5);
        awsConf.setSocketTimeout(10000);
        awsConf.setMaxConnections(1024);
        awsConf.setProtocol(Protocol.HTTPS);

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

        try {

            ListObjectsV2Request req =
                    new ListObjectsV2Request().withBucketName(srcBucket).withPrefix(srcPrefix);
            ListObjectsV2Result result;
            do {
                result = s3Client.listObjectsV2(req);
                for (S3ObjectSummary objectSummary : result.getObjectSummaries()) {
                    // AddTask
                    MigrateAwsTask task = new MigrateAwsTask(config, s3Client,
                            objectSummary.getKey(), objectSummary.getSize(),
                            objectSummary.getETag(), smallFileTransferManager,
                            bigFileTransferManager, recordDb, semaphore);

                    try {
                        AddTask(task);
                    } catch (InterruptedException e) {
                        log.error(e.getMessage());
                    }

                }
                req.setContinuationToken(result.getNextContinuationToken());
            } while (result.isTruncated());
        } catch (AmazonServiceException ase) {
            log.error("list fail AmazonServiceException errorcode: {}, msg: {}", ase.getErrorCode(),
                    ase.getMessage());
        } catch (AmazonClientException ace) {
            log.error("list fail AmazonClientException msg: {}", ace.getMessage().toString());
        }

    }

    @Override
    public void waitTaskOver() {
        super.waitTaskOver();
        this.s3Client.shutdown();
    }
}
