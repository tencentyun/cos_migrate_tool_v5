package com.qcloud.cos_migrate_tool.task;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.file.FileVisitResult;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;

import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.qcloud.cos_migrate_tool.config.CopyFromUrllistConfig;
import com.qcloud.cos_migrate_tool.config.MigrateType;
import com.qcloud.cos_migrate_tool.meta.TaskStatics;
import com.qcloud.cos_migrate_tool.utils.SystemUtils;

public class MigrateUrllistTaskExecutor extends TaskExecutor {

	private static final Logger log = LoggerFactory.getLogger(MigrateLocalTaskExecutor.class);

	private String bucketName;
	private String cosFolder;

	private CopyFromUrllistConfig config;

	public MigrateUrllistTaskExecutor(CopyFromUrllistConfig config) {
		super(MigrateType.MIGRATE_FROM_URLLIST, config);
		this.bucketName = config.getBucketName();
		this.cosFolder = config.getCosPath();

		this.config = config;
	}

	@Override
	public String buildTaskDbComment() {
		String comment = String.format("[time: %s],  [cosFolder: %s]", SystemUtils.getCurrentDateTime(), cosFolder);
		return comment;
	}

	@Override
	public String buildTaskDbFolderPath() {
		String temp = String.format("[urlPath: %s], [cosFolder: %s]", config.getUrllistPath(), cosFolder);
		String dbFolderPath = String.format("db/migrate_from_urllist/%s/%s", bucketName, DigestUtils.md5Hex(temp));
		return dbFolderPath;
	}

	public void buildTask() {

		SimpleFileVisitor<Path> finder = new SimpleFileVisitor<Path>() {
			public FileVisitResult visitFile(Path urllistPath, BasicFileAttributes attrs) throws IOException {

				log.info("ready to migrate url file {}", urllistPath.toAbsolutePath().toString());
				File urllistFile = urllistPath.toFile();
				FileInputStream fis = null;
				try {
					fis = new FileInputStream(urllistFile);
				} catch (FileNotFoundException e) {
					log.error("file not find,msg:{}", e.getMessage());
					return FileVisitResult.CONTINUE;
				}

				BufferedReader br = new BufferedReader(new InputStreamReader(fis));

				String line = null;
				try {
					while ((line = br.readLine()) != null) {
						line = line.trim();
						if (line.isEmpty()) {
							continue;
						}

						String url_path;
						try {
							URL url = new URL(line);
							url_path = url.getPath();
						} catch (Exception e) {
							log.error("parse url fail,line:{} msg:{}", line, e.getMessage());
							TaskStatics.instance.addFailCnt();
							continue;
						}

						MigrateUrllistTask task = new MigrateUrllistTask(config, line, url_path,
								smallFileTransferManager, bigFileTransferManager, recordDb, semaphore,fs);
							
						AddTask(task);
	
					}
					
					TaskStatics.instance.setListFinished(true);
					
				} catch (IOException e) {
					log.error("read line fail,msg:{}", e.getMessage());
					TaskStatics.instance.setListFinished(false);
				} catch (Exception e) {
				    log.error(e.getMessage());
				    TaskStatics.instance.setListFinished(false);
				}

				try {
					br.close();
				} catch (IOException e) {
					log.error("close bufferedreader fail,msg:{}", e.getMessage());
				}

				return super.visitFile(urllistPath, attrs);
			}

		};

		try {
			java.nio.file.Files.walkFileTree(Paths.get(config.getUrllistPath()), finder);
		} catch (IOException e) {
			log.error("walk file tree error", e);
		}

	}

	@Override
	public void waitTaskOver() {
		super.waitTaskOver();
	}

}
