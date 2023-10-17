package com.qcloud.cos_migrate_tool.config;

public enum MigrateType {
    MIGRATE_FROM_LOCAL("migrateLocal"), 
    MIGRATE_FROM_ALI("migrateAli"), 
    MIGRATE_FROM_QINIU("migrateQiniu"), 
    MIGRATE_FROM_AWS("migrateAws"),
    MIGRATE_FROM_URLLIST("migrateUrl"),
    MIGRATE_FROM_COS_BUCKET_COPY("migrateBucketCopy"),
    MIGRATE_FROM_CSP("migrateCsp"),
    MIGRATE_FROM_UPYUN("migrateUpyun"),
    MIGRATE_FROM_LOCAL_TO_COSN_FS("migrateLocalToCosnFs");

    private String migrateType;

    private MigrateType(String migrateType) {
        this.migrateType = migrateType;
    }
    
    public static MigrateType fromValue(String value) throws IllegalArgumentException{
        for (MigrateType elementType : MigrateType.values()) {
            if (elementType.toString().equalsIgnoreCase(value)) {
                return elementType;
            }
        }
        throw new IllegalArgumentException("invalid migrate_type: " + value);
    }

    @Override
    public String toString() {
        return this.migrateType;
    }
}
