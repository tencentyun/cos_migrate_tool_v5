package com.qcloud.cos_migrate_tool.app;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;

import com.qcloud.cos.internal.SkipMd5CheckStrategy;
import com.qcloud.cos_migrate_tool.config.*;
import com.qcloud.cos_migrate_tool.meta.TaskStatics;
import com.qcloud.cos_migrate_tool.task.MigrateAliTaskExecutor;
import com.qcloud.cos_migrate_tool.task.MigrateAwsTaskExecutor;
import com.qcloud.cos_migrate_tool.task.MigrateCopyBucketTaskExecutor;
import com.qcloud.cos_migrate_tool.task.MigrateCspTaskExecutor;
import com.qcloud.cos_migrate_tool.task.MigrateLocalCheckTaskExecutor;
import com.qcloud.cos_migrate_tool.task.MigrateLocalTaskExecutor;
import com.qcloud.cos_migrate_tool.task.MigrateQiniuTaskExecutor;
import com.qcloud.cos_migrate_tool.task.MigrateUpyunTaskExecutor;
import com.qcloud.cos_migrate_tool.task.MigrateUrllistTaskExecutor;
import com.qcloud.cos_migrate_tool.task.TaskExecutor;

import com.qcloud.cos_migrate_tool.hadoop_fs_task.MigrateLocalToCosnTaskExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        } else if (ConfigParser.instance.getMigrateType().equals(MigrateType.MIGRATE_FROM_LOCAL_TO_COSN_FS)) {
            return new MigrateLocalToCosnTaskExecutor((CopyFromLocalToCosnConfig) config);
        } else {
            System.out.println("unknown migrate type");
        }

        return null;
    }

    public static void main(String[] args) {
        // do not calculate md5 
        System.setProperty(SkipMd5CheckStrategy.DISABLE_PUT_OBJECT_MD5_VALIDATION_PROPERTY, "true");

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

                if (config.getCheck()) {
                    TaskStatics.instance.reset();
                    MigrateLocalCheckTaskExecutor checkTaskExecutor = new MigrateLocalCheckTaskExecutor((CopyFromLocalConfig) config);
                    checkTaskExecutor.run();
                    checkTaskExecutor.waitTaskOver();
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
        }
    }
}
