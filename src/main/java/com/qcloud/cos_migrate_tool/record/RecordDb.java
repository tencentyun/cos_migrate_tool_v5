package com.qcloud.cos_migrate_tool.record;

import org.iq80.leveldb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.iq80.leveldb.impl.Iq80DBFactory.*;
import java.io.*;

/**
 * HistoryRecordDb 里存储已经上传的记录, key是Element的key, vaule是Element的value
 * 
 * @author chengwu
 *
 */

public class RecordDb {

    public static final Logger log = LoggerFactory.getLogger(RecordDb.class);

    private static final String ENCODING_TYPE = "UTF-8";
    private static final int CACHE_SIZE = 128 << 20;

    private DB db;

    public RecordDb() {}

    public boolean init(String historyDbFolder, String comment) {
        Options options = new Options();
        options.cacheSize(CACHE_SIZE);
        options.createIfMissing(true);

        try {
            db = factory.open(new File(historyDbFolder), options);
        } catch (IOException e) {
            log.error(e.toString());
            return false;
        }

        String commentFile = historyDbFolder + "/README";
        try {
            BufferedOutputStream bos =
                    new BufferedOutputStream(new FileOutputStream(commentFile, true));
            bos.write(comment.getBytes(ENCODING_TYPE));
            bos.close();
        } catch (FileNotFoundException e) {
            log.error(e.toString());
            return false;
        } catch (IOException e) {
            log.error(e.toString());
            return false;
        }

        return true;
    }

    // 保存记录
    public boolean saveRecord(RecordElement recordElement) {
        String key = recordElement.buildKey();
        String value = recordElement.buildValue();
        return saveKV(key, value);
    }

    public String buildMultipartUploadSavePointKey(String bucketName, String cosKey,
            String localFilePath, long mtime, long partSize, long mutlipartUploadThreshold) {
        String key = String.format(
                "[task_kind: upload_savepoint] [bucket: %s], [key: %s], [localPath: %s], [mtime: %d], [partSize: %d], [mutlipartUploadThreshold: %d]",
                bucketName, cosKey, localFilePath, mtime, partSize, mutlipartUploadThreshold);
        return key;
    }

    public String queryMultipartUploadSavePoint(String bucketName, String cosKey,
            String localFilePath, long mtime, long partSize, long mutlipartUploadThreshold) {
        String key = buildMultipartUploadSavePointKey(bucketName, cosKey, localFilePath, mtime,
                partSize, mutlipartUploadThreshold);
        return queryKV(key);
    }

    public boolean updateMultipartUploadSavePoint(String bucketName, String cosKey,
            String localFilePath, long mtime, long partSize, long mutlipartUploadThreshold,
            String multipartUploadId) {
        String key = buildMultipartUploadSavePointKey(bucketName, cosKey, localFilePath, mtime,
                partSize, mutlipartUploadThreshold);
        return saveKV(key, multipartUploadId);
    }

    public boolean deleteMultipartUploadSavePoint(String bucketName, String cosKey,
            String localFilePath, long mtime, long partSize, long mutlipartUploadThreshold) {
        String key = buildMultipartUploadSavePointKey(bucketName, cosKey, localFilePath, mtime,
                partSize, mutlipartUploadThreshold);
        return deleteKey(key);

    }

    public boolean queryRecord(RecordElement recordElement) {
        String key = recordElement.buildKey();
        String value = queryKV(key);
        if (value == null) {
            return false;
        }
        String recordElementValue = recordElement.buildValue();
        return recordElementValue.equals(value);
    }

    private String queryKV(String key) {
        byte[] valueByte;
        try {
            valueByte = db.get(key.getBytes(ENCODING_TYPE));
        } catch (DBException e) {
            log.error("query db failed, key:{}, exception: {}", key, e.toString());
            return null;
        } catch (UnsupportedEncodingException e) {
            log.error("query db failed, key:{}, exception: {}", key, e.toString());
            return null;
        }

        if (valueByte == null) {
            return null;
        }
        String value;
        try {
            value = new String(valueByte, ENCODING_TYPE);
        } catch (UnsupportedEncodingException e) {
            log.error("query db failed, key:{}, exception: {}", key, e.toString());
            return null;
        }
        return value;
    }

    private boolean saveKV(String key, String value) {
        try {
            db.put(key.getBytes(ENCODING_TYPE), value.getBytes(ENCODING_TYPE));
            return true;
        } catch (DBException e) {
            log.error("update db failed, key:{}, value:{},  exception: {}", key, value,
                    e.toString());
            return false;
        } catch (UnsupportedEncodingException e) {
            log.error("update db failed, key:{}, value:{},  exception: {}", key, value,
                    e.toString());
            return false;
        }
    }
    
    private boolean deleteKey(String key) {
        try {
            db.delete(key.getBytes(ENCODING_TYPE));
            return true;
        } catch (DBException e) {
            log.error("update db failed, key:{}, exception: {}", key, e.toString());
            return false;
        } catch (UnsupportedEncodingException e) {
            log.error("update db failed, key:{},  exception: {}", key, e.toString());
            return false;
        }
    }

    public void shutdown() {
        if (db != null) {
            try {
                db.close();
            } catch (IOException e) {
                log.error("close db occur a exception: " + e.toString());
            }
        }
    }
}
