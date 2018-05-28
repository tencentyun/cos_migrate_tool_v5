package com.qcloud.cos_migrate_tool.utils;

import java.util.HashMap;
import java.util.Map;

public class HeadAttr {
    public long fileSize = -1;
    public String lastModify = "";
    public Map<String, String> userMetaMap = new HashMap<String, String>();
}
