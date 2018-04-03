package com.qcloud.cos_migrate_tool.record;

import com.qcloud.cos_migrate_tool.config.MigrateType;

public class MigrateCompetitorRecordElement extends RecordElement {
    private String bucketName;
    private String etag;
    private String cosPath;
    private long fileSize;

    public MigrateCompetitorRecordElement(MigrateType migrateType, String bucketName,
            String cosPath, String etag, long fileSize) {
        super(migrateType);
        this.bucketName = bucketName;
        this.etag = etag;
        this.cosPath = cosPath;
        this.fileSize = fileSize;
    }

    @Override
    public String buildKey() {
        String key =
                String.format("[taskType: %s] [bucket: %s] [cosPath: %s]",
                		recordType.toString(), bucketName, cosPath);
        return key;
    }

    @Override
    public String buildValue() {
        String value = String.format("[fileSize: %d] [etag: %s]", fileSize, etag);
        return value;
    }
}
