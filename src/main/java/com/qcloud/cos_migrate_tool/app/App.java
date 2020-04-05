package com.qcloud.cos_migrate_tool.app;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.qcloud.cos_migrate_tool.config.CommonConfig;
import com.qcloud.cos_migrate_tool.config.ConfigParser;
import com.qcloud.cos_migrate_tool.config.CopyBucketConfig;
import com.qcloud.cos_migrate_tool.config.CopyFromAliConfig;
import com.qcloud.cos_migrate_tool.config.CopyFromAwsConfig;
import com.qcloud.cos_migrate_tool.config.CopyFromCompetitorConfig;
import com.qcloud.cos_migrate_tool.config.CopyFromLocalConfig;
import com.qcloud.cos_migrate_tool.config.CopyFromQiniuConfig;
import com.qcloud.cos_migrate_tool.config.CopyFromUpyunConfig;
import com.qcloud.cos_migrate_tool.config.CopyFromUrllistConfig;
import com.qcloud.cos_migrate_tool.config.CopyFromCspConfig;
import com.qcloud.cos_migrate_tool.config.MigrateType;
import com.qcloud.cos_migrate_tool.meta.TaskStatics;
import com.qcloud.cos_migrate_tool.task.MigrateAliTaskExecutor;
import com.qcloud.cos_migrate_tool.task.MigrateAwsTaskExecutor;
import com.qcloud.cos_migrate_tool.task.MigrateCopyBucketTaskExecutor;
import com.qcloud.cos_migrate_tool.task.MigrateCspTaskExecutor;
import com.qcloud.cos_migrate_tool.task.MigrateLocalTaskExecutor;
import com.qcloud.cos_migrate_tool.task.MigrateQiniuTaskExecutor;
import com.qcloud.cos_migrate_tool.task.MigrateUpyunTaskExecutor;
import com.qcloud.cos_migrate_tool.task.MigrateUrllistTaskExecutor;
import com.qcloud.cos_migrate_tool.task.TaskExecutor;

public class App {

    private static final Logger log = LoggerFactory.getLogger(App.class);

    private static TaskExecutor buildTaskExecutor(CommonConfig config) {
        if (ConfigParser.instance.getMigrateType().equals(MigrateType.MIGRATE_FROM_LOCAL)) {
            return new MigrateLocalTaskExecutor((CopyFromLocalConfig) config);
        } else if (ConfigParser.instance.getMigrateType().equals(MigrateType.MIGRATE_FROM_ALI)) {
            return new MigrateAliTaskExecutor((CopyFromAliConfig) config);
        } else if (ConfigParser.instance.getMigrateType().equals(MigrateType.MIGRATE_FROM_AWS)) {
            return new MigrateAwsTaskExecutor((CopyFromAwsConfig) config);
        } else if (ConfigParser.instance.getMigrateType()
                .equals(MigrateType.MIGRATE_FROM_COS_BUCKET_COPY)) {
            return new MigrateCopyBucketTaskExecutor((CopyBucketConfig) config);
        } else if (ConfigParser.instance.getMigrateType()
                .equals(MigrateType.MIGRATE_FROM_URLLIST)) {
            return new MigrateUrllistTaskExecutor((CopyFromUrllistConfig) config);
        } else if (ConfigParser.instance.getMigrateType().equals(MigrateType.MIGRATE_FROM_QINIU)) {
            return new MigrateQiniuTaskExecutor((CopyFromQiniuConfig) config);
        } else if (ConfigParser.instance.getMigrateType().equals(MigrateType.MIGRATE_FROM_CSP)) {
            return new MigrateCspTaskExecutor((CopyFromCspConfig) config);
        } else if (ConfigParser.instance.getMigrateType().equals(MigrateType.MIGRATE_FROM_UPYUN)) {
            return new MigrateUpyunTaskExecutor((CopyFromUpyunConfig) config);
        } else {
            System.out.println("unknown migrate type");
        }

        return null;
    }

    public static void main(String[] args) {
        if(0 < args.length) {
            ConfigParser.setConfigFilePath(args[0]);
        }
        if (!ConfigParser.instance.parse()) {
            return;
        }

        CommonConfig config = ConfigParser.instance.getConfig();

        String batchTaskPath = config.getBatchTaskPath();
        if (!batchTaskPath.isEmpty()) {

            FileInputStream inputStream = null;
            try {
                inputStream = new FileInputStream(batchTaskPath);
            } catch (FileNotFoundException e) {
                e.printStackTrace();
                return;
            }
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));

            String str = null;
            try {
                while (true) {
                    while ((str = bufferedReader.readLine()) != null) {
                        String[] task = str.split("\t");
                        if (!((CopyFromCompetitorConfig) config).setTask(task)) {
                            System.out.printf("task:%s invalid\n", str);
                            log.error("task:{} invalid", str);
                            continue;
                        }

                        TaskStatics.instance.reset();

                        TaskExecutor taskExecutor = buildTaskExecutor(config);
                        taskExecutor.run();
                        taskExecutor.waitTaskOver();
                    }
                    
                    if (!config.isDamonMode())
                        break;

                    try {
                        Thread.sleep(config.getDamonInterVal() * 1000);
                    } catch (InterruptedException e) {
                        log.error("the program is interrupted!", e);
                        break;
                    }
                }

                inputStream.close();
                bufferedReader.close();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
                return;
            }

        } else {

            while (true) {
                TaskStatics.instance.reset();

                TaskExecutor taskExecutor = buildTaskExecutor(config);
                taskExecutor.run();
                taskExecutor.waitTaskOver();

                if (!config.isDamonMode())
                    break;

                try {
                    Thread.sleep(config.getDamonInterVal() * 1000);
                } catch (InterruptedException e) {
                    log.error("the program is interrupted!", e);
                    break;
                }
            }
        }
    }
}
