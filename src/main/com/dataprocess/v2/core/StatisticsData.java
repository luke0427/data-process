package com.ropeok.dataprocess.v2.core;

import org.apache.commons.lang3.time.StopWatch;

import java.util.LinkedList;
import java.util.List;

public class StatisticsData {
    private long totalRow;
    private long currentRow;
//    private long costTime;
    private String name;
    StopWatch stopWatch = new StopWatch();
    private List<StatisticsData> lists = new LinkedList<>();

    public void start() {
        stopWatch.start();
    }

    public StatisticsData(String name) {
        this.name = name;
        init();
    }

    public void addStat(StatisticsData stat) {
        lists.add(stat);
        totalRow += stat.getTotalRow();
    }

    public void init() {
        totalRow = 0;
        currentRow = 0;
    }

    public void reset() {
        currentRow = 0;
    }

    public long increRow() {
        totalRow++;
        return ++currentRow;
    }

    public String getName() {
        return this.name;
    }

    public long getTotalRow() {
        return totalRow;
    }

    public long getCurrentRow() {
        return currentRow;
    }

//    public void stop() {
//        this.costTime = stopWatch.getTime();
//    }

    public long getTime() {
        return stopWatch.getTime();
    }

    public String getCurrentInfo() {
        long speed = (this.totalRow*1000)/stopWatch.getTime();
        return String.format("已处理行数=%d，已耗时%d毫秒, 速度:%d条/秒", this.totalRow, this.getCostTime(), (speed > 0 ? speed : this.totalRow));
    }

    public long getCostTime() {
        return stopWatch.getTime();
    }

    @Override
    public String toString() {
        /*if(costTime == 0) {
            stop();
        }*/
        StringBuilder info = new StringBuilder();
        for(StatisticsData sd : lists) {
            long speed = (sd.getTotalRow()*1000)/sd.getCostTime();
            info.append(String.format("[%s]处理行数=%d，共耗时%d毫秒, 速度:%d条/秒\n", sd.getName(), sd.getTotalRow(), sd.getCostTime(), speed > 0 ? speed : sd.getTotalRow()));
        }
        info.append(String.format("[%s]处理行数=%d，共耗时%d毫秒", this.name, this.totalRow, this.getCostTime()));
        stopWatch.stop();
        return info.toString();
    }
}
