package com.qcloud.cos_migrate_tool.meta;

import java.util.concurrent.atomic.AtomicLong;

import org.joda.time.DateTime;
import org.joda.time.Seconds;

/**
 * 用于统计各类任务的数量，并将每次执行的任务统计结果打印到本机, 并写入数据库
 * 
 * @author chengwu
 *
 */
public class TaskStatics {
    public static final TaskStatics instance = new TaskStatics();
    private DateTime startTime = DateTime.now();

    // 上传文件的总量, 成功量, 失败量
    private AtomicLong successCnt = new AtomicLong(0L);
    private AtomicLong failCnt = new AtomicLong(0L);
    private AtomicLong skipCnt = new AtomicLong(0L);

    private TaskStatics() {}

    public void addSuccessCnt() {
        this.successCnt.incrementAndGet();
    }

    public long getSuccessCnt() {
        return this.successCnt.get();
    }

    public void addFailCnt() {
        this.failCnt.incrementAndGet();
    }

    public long getFailCnt() {
        return this.failCnt.get();
    }

    public void addSkipCnt() {
        this.skipCnt.incrementAndGet();
    }

    public long getSkipCnt() {
        return this.skipCnt.get();
    }

    public String getStartTimeStr() {
        return this.startTime.toString("yyyy-MM-dd HH:mm:ss");
    }

    public int getUsedTimeSeconds() {
        DateTime enDateTime = DateTime.now();
        Seconds seconds = Seconds.secondsBetween(this.startTime, enDateTime);
        return seconds.getSeconds();
    }
    
    public void reset() {
        this.startTime =DateTime.now();
        this.successCnt.set(0);
        this.failCnt.set(0);
        this.skipCnt.set(0);
    }
}
