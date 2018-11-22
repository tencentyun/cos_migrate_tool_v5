package com.qcloud.cos_migrate_tool.test;

import java.util.Properties;
import java.util.Iterator;
import org.junit.Before;
import org.junit.Test;
import com.qcloud.cos_migrate_tool.thirdparty.upyun.UpYun;

public class TestUpyunAPI {
    UpYun upyun = null;

    @Before
    public void setUp() throws Exception {
        Properties prop = new Properties();
        prop.load(getClass().getClassLoader().getResourceAsStream("upyunlogin.properties"));
        upyun = new UpYun(prop.getProperty("bucket"), prop.getProperty("username"), prop.getProperty("password"));

        String proxyServer = prop.getProperty("proxyServer");
        int proxyPort = Integer.parseInt(prop.getProperty("proxyPort"));

        if (proxyServer.length() > 0) {
            upyun.setProxy(proxyServer, proxyPort);
        }
    }

    private void readDir(String path) throws Exception {
        System.out.println("reading " + path);
        String dir = path;
        UpYun.ReadDirResult rr = upyun.readDir(dir, null, 10);
        while (rr.items.size() > 0) {
            for(Iterator<UpYun.FolderItem> i = rr.items.iterator(); i.hasNext(); ) {
                UpYun.FolderItem item = i.next();
                System.out.println(dir + item.name);
                if (item.type == "Folder") {
                    readDir(path + item.name + "/");
                }
            }
            rr = upyun.readDir(dir, rr.nextIter, 10);
        }
    }

    @Test
    public void testReadDir() throws Exception {
        // readDir("/");
    }
}