package com.qcloud.cos_migrate_tool.utils;

import com.qcloud.cos.model.Grantee;

public class TceGrantee implements Grantee {
    
    public String type = "";
    public String id = "";

    public String getTypeIdentifier() {
        return type;
    }
    
    public void setIdentifier(String id) {
        this.id = id;
    }
    
    public void setTypeIdentifier(String type) {
        this.type = type;
    }

    public String getIdentifier() {
        return id;
    }
}
