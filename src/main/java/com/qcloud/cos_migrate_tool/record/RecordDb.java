package com.qcloud.cos_migrate_tool.record;

import com.qcloud.cos_migrate_tool.config.CommonConfig;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.LinkedList;

import org.rocksdb.FlushOptions;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.util.SizeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.qcloud.cos.utils.IOUtils;

/**
 * HistoryRecordDb 里存储已经上传的记录, key是Element的key, vaule是Element的value
 * 
 * @author chengwu
 *
 */

public class RecordDb {

    public enum QUERY_RESULT {
        KEY_NOT_EXIST, VALUE_NOT_EQ, ALL_EQ;
    }
    
    public static final Logger log = LoggerFactory.getLogger(RecordDb.class);

    private static final String ENCODING_TYPE = "UTF-8";

    private RocksDB db;
    private Options options;
    private final String requestIdPrefix = "x-cos-requestId-";
    private String dbFolder;


    public RecordDb() {}

    public boolean init(DBParam dbParam) {
        log.info("init db with :" + dbParam);
        try {
            dbFolder = dbParam.getHistoryFolder();
            options = new Options();
            options.setCreateIfMissing(true);
            options.setWriteBufferSize(16 * SizeUnit.MB).setMaxWriteBufferNumber(4).setMaxBackgroundCompactions(4);

            if (CommonConfig.isRocksDBMaxOpenFileValid(dbParam.getMaxOpenFile())) {
                // 这里先不给默认值，稳定后再给默认值
                options.setMaxOpenFiles(dbParam.getMaxOpenFile());
            }

            db = RocksDB.open(options, dbParam.getHistoryFolder());
        } catch (RocksDBException e) {
            log.error(e.toString());
            return false;
        }

        String commentFile = dbParam.getHistoryFolder() + "/README";
        try {
            BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(commentFile, true));
            bos.write(dbParam.getComment().getBytes(ENCODING_TYPE));
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
    
    public boolean  saveListProgress(String prefix, String marker) {
        String value = prefix + "|" + marker;
        return saveKV("listProgress", value);
    }
    
    
    public String[] getListProgress() {
        String value = queryKV("listProgress");
        if (value == null) {
            return null;
        } 

        int i = value.lastIndexOf("|");
        if (i < 0) {
            return null;
        }

        String prefix = value.substring(0, i);
        String marker = "";
        if (i != value.length()-1) {
            marker = value.substring(i+1, value.length()-1);
        }
        
        String[] arr1 = new String[] {prefix, marker};
        return arr1;
        
    }
    
    public boolean saveDirProgress(String curDir, String lastItr, LinkedList<String> dirList) {

        String progressFile = this.dbFolder + "/PROGRESS";

        try {
            BufferedOutputStream bos =
                    new BufferedOutputStream(new FileOutputStream(progressFile, false));
            bos.write(lastItr.getBytes(ENCODING_TYPE));
            bos.write("\n".getBytes(ENCODING_TYPE));
            bos.write(curDir.getBytes(ENCODING_TYPE));
            bos.write("\n".getBytes(ENCODING_TYPE));
            for (String x: dirList) {
                bos.write(x.getBytes(ENCODING_TYPE));
                bos.write("\n".getBytes(ENCODING_TYPE));
            }

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
    
    public LinkedList<String> getDirProgress() {
        String progressFile = this.dbFolder + "/PROGRESS";
        LinkedList<String> result = new LinkedList<String>();
        try {
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream(progressFile)));
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                result.addLast(line);
                log.info("[{}]", line);
            }
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            //log.error(e.toString());
            //e.printStackTrace();
            return null;
        } catch (IOException e) {
            // TODO Auto-generated catch block
            log.error(e.toString());
            e.printStackTrace();
            return null;
        }
        return result;
    }

    // 保存记录
    public boolean saveRecord(RecordElement recordElement) {
        String key = recordElement.buildKey();
        String value = recordElement.buildValue();
        return saveKV(key, value);
    }

    public boolean saveRequestId(String cosKey, String requestId) {
        String dbKey = requestIdPrefix + cosKey;
        if (requestId == null) {
            log.warn("requestId is null for cosKey " + cosKey);
            return saveKV(dbKey, "Null");
        } else {
            return saveKV(dbKey, requestId);
        }
    }    
    
    public void dumpRequestId(String saveFilePath) {
        ReadOptions readOptions = null;
        RocksIterator rocksIterator = null;
        BufferedOutputStream bos = null;
        try {
            bos = new BufferedOutputStream(new FileOutputStream(saveFilePath));
            readOptions = new ReadOptions();
            rocksIterator = db.newIterator(readOptions);
            rocksIterator.seek(requestIdPrefix.getBytes(ENCODING_TYPE));
            while (rocksIterator.isValid()) {
                String key = new String(rocksIterator.key(), ENCODING_TYPE).trim();
                key = key.substring(requestIdPrefix.length());
                String value = new String(rocksIterator.value(), ENCODING_TYPE).trim();
                String content = String.format("%s \t %s\n", key, value);
                bos.write(content.getBytes(ENCODING_TYPE));
                rocksIterator.next();
            }
        } catch (Exception e) {
            final String errMsg = "dumpRequestId error.";
            System.err.println(errMsg);
            log.error(errMsg, e);
        } finally {
            if (readOptions != null) {
                readOptions.close();
            }
            if (rocksIterator != null) {
                rocksIterator.close();
            }
            if (bos != null) {
                IOUtils.closeQuietly(bos, log);
            }
        }
    }


    
    public void queryRequestId(String cosKey) {
        String dbKey = requestIdPrefix + cosKey;
        String requestIdValue = queryKV(dbKey);
        if (requestIdValue == null) {
            requestIdValue = "Null";
        }
        String infoMsg = String.format("query requestid, [key: %s], [requestid: %s]", cosKey,
                requestIdValue);
        System.out.println(infoMsg);
        log.info(infoMsg);
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

    public QUERY_RESULT queryRecord(RecordElement recordElement) {
        String key = recordElement.buildKey();
        String value = null;
        try {
            value = queryKV(key);
        } catch (Exception e) {
            log.error("query kv occur a exception: ", e);
            return QUERY_RESULT.KEY_NOT_EXIST;
        }
        if (value == null) {
            return QUERY_RESULT.KEY_NOT_EXIST;
        }
        
        String recordElementValue = recordElement.buildValue();
        if (recordElementValue.equals(value)) {
            return QUERY_RESULT.ALL_EQ;
        }

        log.info("obj had update,old_value:{} current_value:{}", value, recordElementValue);
        return QUERY_RESULT.VALUE_NOT_EQ;
    }

    private String queryKV(String key) {
        byte[] valueByte;
        try {
            valueByte = db.get(key.getBytes(ENCODING_TYPE));
        } catch (RocksDBException e) {
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
        } catch (RocksDBException e) {
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
        } catch (RocksDBException e) {
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
                FlushOptions flushOptions = new FlushOptions();
                flushOptions.setWaitForFlush(true);
                db.flush(flushOptions);
                flushOptions.close();
                db.close();
                if (options != null) {
                    options.close();
                }
            } catch (RocksDBException e) {
                log.error("close db occur a exception: " + e.toString());
            }
        }
    }
}
