package com.qcloud.cos_migrate_tool.app;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.qcloud.cos_migrate_tool.config.CommonConfig;
import com.qcloud.cos_migrate_tool.config.ConfigParser;
import com.qcloud.cos_migrate_tool.config.CopyBucketConfig;
import com.qcloud.cos_migrate_tool.config.CopyFromAliConfig;
import com.qcloud.cos_migrate_tool.config.CopyFromAwsConfig;
import com.qcloud.cos_migrate_tool.config.CopyFromLocalConfig;
import com.qcloud.cos_migrate_tool.config.CopyFromQiniuConfig;
import com.qcloud.cos_migrate_tool.config.CopyFromUrllistConfig;
import com.qcloud.cos_migrate_tool.config.CopyFromUpyunConfig;
import com.qcloud.cos_migrate_tool.config.MigrateType;
import com.qcloud.cos_migrate_tool.meta.TaskStatics;
import com.qcloud.cos_migrate_tool.task.MigrateAliTaskExecutor;
import com.qcloud.cos_migrate_tool.task.MigrateAwsTaskExecutor;
import com.qcloud.cos_migrate_tool.task.MigrateCopyBucketTaskExecutor;
import com.qcloud.cos_migrate_tool.task.MigrateLocalTaskExecutor;
import com.qcloud.cos_migrate_tool.task.MigrateQiniuTaskExecutor;
import com.qcloud.cos_migrate_tool.task.MigrateUrllistTaskExecutor;
import com.qcloud.cos_migrate_tool.task.MigrateUpyunTaskExecutor;
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
        } else if (ConfigParser.instance.getMigrateType().equals(MigrateType.MIGRATE_FROM_UPYUN)) {
            return new MigrateUpyunTaskExecutor((CopyFromUpyunConfig) config);
        } else if (ConfigParser.instance.getMigrateType()
                .equals(MigrateType.MIGRATE_FROM_COS_BUCKET_COPY)) {
            return new MigrateCopyBucketTaskExecutor((CopyBucketConfig) config);
        } else if (ConfigParser.instance.getMigrateType()
                .equals(MigrateType.MIGRATE_FROM_URLLIST)) {
            return new MigrateUrllistTaskExecutor((CopyFromUrllistConfig) config);
        } else if (ConfigParser.instance.getMigrateType().equals(MigrateType.MIGRATE_FROM_QINIU)) {
            return new MigrateQiniuTaskExecutor((CopyFromQiniuConfig) config);
        } else {
            System.out.println("unknown migrate type");
        }

        return null;
    }

    public static void main(String[] args) {
        if (!ConfigParser.instance.parse()) {
            return;
        }

        CommonConfig config = ConfigParser.instance.getConfig();
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
