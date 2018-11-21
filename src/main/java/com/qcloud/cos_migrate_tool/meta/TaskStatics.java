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
    private AtomicLong conditionNotMatchCnt = new AtomicLong(0L);
    private AtomicLong updateCnt = new AtomicLong(0L);
    private boolean list_finished = false;
    
    private TaskStatics() {}
    
    public void setListFinished(boolean is_finished) {
        this.list_finished = is_finished;
    }
    
    public boolean getListFinished() {
        return this.list_finished;
    }
    
    public void addUpdateCnt() {
        this.updateCnt.incrementAndGet();
    }
    
    public long getUpdateCnt() {
        return this.updateCnt.get();
    }

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
    
    public void addConditionNotMatchCnt() {
        this.conditionNotMatchCnt.incrementAndGet();
    }
    
    public long getConditionNotMatchCnt() {
        return this.conditionNotMatchCnt.get();
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
        this.updateCnt.set(0);
        this.conditionNotMatchCnt.set(0);
        this.list_finished = false;
        
    }
}
