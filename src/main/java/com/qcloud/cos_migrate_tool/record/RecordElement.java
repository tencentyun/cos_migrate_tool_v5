package com.qcloud.cos_migrate_tool.record;

import com.qcloud.cos_migrate_tool.config.MigrateType;

public abstract class RecordElement {
    protected MigrateType recordType;

    public RecordElement(MigrateType recordType) {
        super();
        this.recordType = recordType;
    }

    public abstract String buildKey();

    public abstract String buildValue();

    public static RecordElement parseRecord(String key, String value) {
        return null;
    }

    @Override
    public String toString() {
        String str = String.format("[record_type: %s], [key: %s], [value: %s]",
                recordType.toString(), buildKey(), buildValue());
        return str;
    }
}
