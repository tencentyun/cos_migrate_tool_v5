package com.qcloud.cos_migrate_tool.test;

import java.util.Properties;
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
    }

    @Test
    public void testReadDir() {

    }
}