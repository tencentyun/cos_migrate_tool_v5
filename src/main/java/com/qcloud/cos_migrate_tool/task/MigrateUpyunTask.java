package com.qcloud.cos_migrate_tool.task;

import java.io.File;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;

import com.qcloud.cos.model.ObjectMetadata;
import com.qcloud.cos.transfer.TransferManager;
import com.qcloud.cos_migrate_tool.config.CopyFromUpyunConfig;
import com.qcloud.cos_migrate_tool.config.MigrateType;
import com.qcloud.cos_migrate_tool.meta.TaskStatics;
import com.qcloud.cos_migrate_tool.record.MigrateCompetitorRecordElement;
import com.qcloud.cos_migrate_tool.record.RecordDb;
import com.qcloud.cos_migrate_tool.utils.Downloader;
import com.qcloud.cos_migrate_tool.utils.HeadAttr;

import com.qcloud.cos_migrate_tool.thirdparty.upyun.UpYun;

public class MigrateUpyunTask extends Task {
	private String srcPath;
	private long fileSize;
	private String etag;

	public MigrateUpyunTask(CopyFromUpyunConfig config, String srcPath, long fileSize, String etag,
			TransferManager smallFileTransfer, TransferManager bigFileTransfer, RecordDb recordDb,
			Semaphore semaphore) {
	    super(semaphore, config, smallFileTransfer, bigFileTransfer, recordDb);
		this.config = config;
		this.srcPath = srcPath;
		this.fileSize = fileSize;
		this.etag = etag;
	}

	private String buildCOSPath() {
		if (this.srcPath.startsWith("/")) {
            return this.srcPath.substring(1);
        }
		return this.srcPath;
	}

	@Override
	public void doTask() {

		String cosPath = buildCOSPath();

		String localPath = config.getTempFolderPath() + ThreadLocalRandom.current().nextLong();

		MigrateCompetitorRecordElement upyunRecordElement = new MigrateCompetitorRecordElement(
				MigrateType.MIGRATE_FROM_UPYUN, config.getBucketName(), cosPath, etag, fileSize);
		if (isExist(upyunRecordElement)) {
			TaskStatics.instance.addSkipCnt();
			return;
		}

		File localFile = new File(localPath);

		CopyFromUpyunConfig config = (CopyFromUpyunConfig)this.config;

    	UpYun upyun = new UpYun(config.getSrcBucket(), config.getSrcAccessKeyId(), config.getSrcAccessKeySecret());
         if (!config.getSrcProxyHost().isEmpty() && config.getSrcProxyPort() > 0) {
            upyun.setProxy(config.getSrcProxyHost(), config.getSrcProxyPort());;
		}
		
		try {
			upyun.readFile(this.srcPath, localFile);
		} catch (Exception e) {
			String errMsg = String.format("[fail] taskInfo: %s. download to %s failed, srcPath: %s",
				upyunRecordElement.buildKey(), localPath, srcPath);
			System.err.println(errMsg);
			log.error(errMsg);
			TaskStatics.instance.addFailCnt();
			return;
		}

		// upload
		if (!localFile.exists()) {
			String errMsg = String.format("[fail] taskInfo: %s. file: %s not exist, srcPath: %s",
				upyunRecordElement.buildKey(), localPath, srcPath);
			System.err.println(errMsg);
			log.error(errMsg);
			TaskStatics.instance.addFailCnt();
			return;
		}
		
		if (localFile.length() != this.fileSize) {
			log.error("download size[{}] != list size[{}]", localFile.length(), this.fileSize);
			TaskStatics.instance.addFailCnt();
			return;
		}

		try {
		    com.qcloud.cos.model.ObjectMetadata objectMetadata = new com.qcloud.cos.model.ObjectMetadata();
		    objectMetadata.addUserMetadata("upyun-etag", etag);
		    
			String requestId = uploadFile(config.getBucketName(), cosPath, localFile, config.getStorageClass(),
					config.isEntireFileMd5Attached(), objectMetadata);
			saveRecord(upyunRecordElement);
			saveRequestId(cosPath, requestId);
            if (this.query_result == RecordDb.QUERY_RESULT.KEY_NOT_EXIST) {
                TaskStatics.instance.addSuccessCnt();
            } else {
                TaskStatics.instance.addUpdateCnt();
            }
			String printMsg = String.format("[ok] [requestid: %s], task_info: %s", requestId == null ? "NULL" : requestId, upyunRecordElement.buildKey());
			System.out.println(printMsg);
			log.info(printMsg);
		} catch (Exception e) {
			String printMsg = String.format("[fail] task_info: %s", upyunRecordElement.buildKey());
			System.err.println(printMsg);
			log.error("[fail] task_info: {}, exception: {}", upyunRecordElement.buildKey(), e.toString());
			TaskStatics.instance.addFailCnt();
		} finally {
			localFile.delete();
		}
	}
}
