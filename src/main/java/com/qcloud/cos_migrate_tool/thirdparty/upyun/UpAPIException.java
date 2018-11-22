package com.qcloud.cos_migrate_tool.thirdparty.upyun;

public class UpAPIException extends UpException {
    public int statusCode;

    public UpAPIException(int statusCode, String msg) {
        super("api error code:" + statusCode + "; msg:" + msg);
        this.statusCode = statusCode;
    }

}
