package com.qcloud.cos_migrate_tool.config;


public class CopyFromUpyunConfig extends CopyFromCompetitorConfig {
    private boolean compareMd5 = true;
    private boolean isAsc = true;
    
    public boolean isCompareMd5() {
        return compareMd5;
    }
 	
    public void setCompareMd5(boolean compareMd5) {
        this.compareMd5 = compareMd5;
    }
    
    public void setAscendingOrder(String isAsc) {
        if (isAsc.compareToIgnoreCase("false") == 0) {
            this.isAsc = false;
        } else if (isAsc.compareToIgnoreCase("true") != 0) {
            throw new IllegalArgumentException("ascendingOrder invalid.should be true or false");
        }
    }
    
    public boolean isAscendingOrder() {
        return this.isAsc;
    }
}
