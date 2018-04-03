package com.qcloud.cos_migrate_tool.record;

import com.qcloud.cos_migrate_tool.config.MigrateType;

public class MigrateUrllistRecordElement extends RecordElement {
    private String bucketName;
    private String cosPath;
    private String url;
    private long fileSize;

    public MigrateUrllistRecordElement(MigrateType migrateType, String bucketName, String cosPath,
            String url, long fileSize) {
        super(migrateType);
        this.bucketName = bucketName;
        this.cosPath = cosPath;
        this.url = url;
        this.fileSize = fileSize;
    }

    @Override
    public String buildKey() {
        String key = String.format("[taskType: %s] [bucket: %s] [cosPath: %s] [url: %s]",
                recordType.toString(), bucketName, cosPath, url);
        return key;
    }

    @Override
    public String buildValue() {
        String value = String.format("[fileSize: %d]", fileSize);
        return value;
    }
}
