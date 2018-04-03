package com.qcloud.cos_migrate_tool.utils;

public class PathUtils {
    private static final String PATH_DELIMITER = "/";

    public static String formatCosFolderPath(String cosPath) {
        cosPath = cosPath.trim();
        if (!cosPath.startsWith(PATH_DELIMITER)) {
            cosPath = PATH_DELIMITER + cosPath;
        }

        cosPath = cosPath.replaceAll("//", PATH_DELIMITER);

        if (cosPath.endsWith(PATH_DELIMITER)) {
            return cosPath;
        } else {
            return cosPath + PATH_DELIMITER;
        }
    }
}
