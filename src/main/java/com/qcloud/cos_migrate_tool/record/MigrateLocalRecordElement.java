package com.qcloud.cos_migrate_tool.record;

import com.qcloud.cos_migrate_tool.config.MigrateType;

public class MigrateLocalRecordElement extends RecordElement {
    private String bucketName;
    private String localPath;
    private String cosPath;
    private long mtime;
    private long fileSize;

    public MigrateLocalRecordElement(String bucketName,
            String localPath, String cosPath, long mtime, long fileSize) {
        super(MigrateType.MIGRATE_FROM_LOCAL);
        this.bucketName = bucketName;
        this.localPath = localPath;
        this.cosPath = cosPath;
        this.mtime = mtime;
        this.fileSize = fileSize;
    }



    @Override
    public String buildKey() {
        String key = String.format("[taskType: %s] [bucket: %s], [localPath: %s], [cosPath: %s]", recordType.toString(), bucketName,
                localPath, cosPath);
        return key;
    }

    @Override
    public String buildValue() {
        String value = String.format("[mtime: %d], [fileSize: %d]", mtime,  fileSize);
        return value;
    }
}
