package com.qcloud.cos_migrate_tool.config;


public class CopyFromUpyunConfig extends CopyFromCompetitorConfig {
    private boolean compareMd5 = true;
    
    public boolean isCompareMd5() {
        return compareMd5;
    }
 	
    public void setCompareMd5(boolean compareMd5) {
        this.compareMd5 = compareMd5;
    }
}
