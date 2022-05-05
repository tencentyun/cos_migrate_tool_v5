package com.qcloud.cos_migrate_tool.record;

public class DBParam {
    public DBParam(String historyFolder, String comment, int maxOpenFile) {
        this.historyFolder = historyFolder;
        this.comment = comment;
        this.maxOpenFile = maxOpenFile;
    }

    @Override
    public String toString() {
        return "DBParam{" +
                "historyFolder='" + historyFolder + '\'' +
                ", comment='" + comment + '\'' +
                ", maxOpenFile=" + maxOpenFile +
                '}';
    }

    private final String historyFolder;
    private final String comment;
    private final int maxOpenFile;

    public String getHistoryFolder() {
        return historyFolder;
    }

    public String getComment() {
        return comment;
    }

    public int getMaxOpenFile() {
        return maxOpenFile;
    }
}
