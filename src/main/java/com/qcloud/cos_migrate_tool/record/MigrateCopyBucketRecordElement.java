package com.qcloud.cos_migrate_tool.record;

import com.qcloud.cos_migrate_tool.config.MigrateType;

public class MigrateCopyBucketRecordElement extends RecordElement {
    private String destRegion;
    private String destBucketName;
    private String destKey;
    private String srcRegion;
    private String srcBucketName;
    private String srcKey;
    private long srcSize;
    private String srcEtag;
    private String srcStorageClass;



    public MigrateCopyBucketRecordElement(String destRegion, String destBucketName, String destKey,
            String srcRegion, String srcBucketName, String srcKey, long srcSize, String srcEtag, String srcStorageClass) {
        super(MigrateType.MIGRATE_FROM_COS_BUCKET_COPY);
        this.destRegion = destRegion;
        this.destBucketName = destBucketName;
        this.destKey = destKey;
        this.srcRegion = srcRegion;
        this.srcBucketName = srcBucketName;
        this.srcKey = srcKey;
        this.srcSize = srcSize;
        this.srcEtag = srcEtag;
        this.srcStorageClass = srcStorageClass;
    }

    @Override
    public String buildKey() {
        String key = String.format(
                "[taskType: %s] [destRegion: %s], [destBucketName: %s], [destKey: %s], [srcRegion: %s], [srcBucketName: %s], [srcKey: %s], [srcStorageClass: %s]",
                recordType.toString(), destRegion, destBucketName, destKey, srcRegion, srcBucketName, srcKey, srcStorageClass);
        return key;
    }

    @Override
    public String buildValue() {
        String value = String.format("[srcSize: %d], [srcEtag: %s], [srcStorageClass: %s]", srcSize, srcEtag, srcStorageClass);
        return value;
    }

}
