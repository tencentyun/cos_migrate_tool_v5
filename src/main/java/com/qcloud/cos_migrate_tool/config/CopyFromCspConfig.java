package com.qcloud.cos_migrate_tool.config;


public class CopyFromCspConfig extends CopyFromCompetitorConfig {
    private boolean migrateSlashEndObjectAcl = true;


    public boolean isMigrateSlashEndObjectAcl() {
        return migrateSlashEndObjectAcl;
    }

    public void setMigrateSlashEndObjectAcl(boolean migrateSlashEndObjectAcl) {
        this.migrateSlashEndObjectAcl = migrateSlashEndObjectAcl;
    }
}
