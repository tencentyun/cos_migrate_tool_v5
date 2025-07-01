package com.qcloud.cos_migrate_tool.utils;

import java.io.InputStream;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VersionInfoUtils {
    /** The version info file with versioning info */
    static final String VERSION_INFO_FILE = "/toolVersionInfo.properties";

    /** version info */
    private static volatile String version;

    /** platform info */
    private static volatile String platform;

    /** User Agent info */
    private static volatile String userAgent;

    /** Shared logger for any issues while loading version information */
    private static final Logger log = LoggerFactory.getLogger(VersionInfoUtils.class);

    /**
     * Returns the current version for the cos migrate tool in which this class is running. Version
     * information is obtained from from the versionInfo.properties file which the cos migrate tool
     * build process generates.
     *
     * @return The current version for the cos migrate tool, if known, otherwise returns a string indicating
     *         that the version information is not available.
     */
    public static String getVersion() {
        if (version == null) {
            synchronized (VersionInfoUtils.class) {
                if (version == null)
                    initializeVersion();
            }
        }
        return version;
    }

    /**
     * Returns the current platform for the cos migrate tool in which this class is running. Version
     * information is obtained from from the versionInfo.properties file which the cos migrate tool
     * build process generates.
     *
     * @return The current platform for the cos migrate tool, if known, otherwise returns a string indicating
     *         that the platform information is not available.
     */
    public static String getPlatform() {
        if (platform == null) {
            synchronized (VersionInfoUtils.class) {
                if (platform == null)
                    initializeVersion();
            }
        }
        return platform;
    }

    /**
     * @return Returns the User Agent string to be used when communicating with the COS services.
     *         The User Agent encapsulates SDK, Java, OS and region information.
     *         eg:cos-migrate-tool-v${project.version}/Linux/jdk-1.8.0_271/Java HotSpot(TM) 64-Bit Server VM=
     */
    public static String getUserAgent() {
        if (userAgent == null) {
            synchronized(VersionInfoUtils.class) {
                if (userAgent == null) {
                    userAgent =  String.format("cos-migrate-tool-v%s/%s/jdk-%s/%s",
                                               getVersion(),
                                               System.getProperty("os.name"),
                                               System.getProperty("java.version"),
                                               System.getProperty("java.vm.name"));
                } 
            }
        }
        return userAgent;
    }

    /**
     * Loads the versionInfo.properties file from the cos migrate tool and stores the information so
     * that the file doesn't have to be read the next time the data is needed.
     */
    private static void initializeVersion() {
        InputStream inputStream = VersionInfoUtils.class.getResourceAsStream(VERSION_INFO_FILE);
        Properties versionInfoProperties = new Properties();
        try {
            if (inputStream == null)
                throw new Exception(VERSION_INFO_FILE + " not found on classpath");

            versionInfoProperties.load(inputStream);
            version = versionInfoProperties.getProperty("version");
        } catch (Exception e) {
            log.info("Unable to load version information for the running migrate tool: " + e.getMessage());
            version = "unknown-version";
            platform = "java";
        } finally {
            try {
                inputStream.close();
            } catch (Exception e) {
            }
        }
    }
}