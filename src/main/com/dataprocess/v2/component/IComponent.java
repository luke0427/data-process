package com.ropeok.dataprocess.v2.component;

import com.ropeok.dataprocess.v2.core.SendEvent;
import com.ropeok.dataprocess.v2.core.StatisticsData;

public interface IComponent extends Runnable{
    public int getId();
    public String getName();
    public void init() throws Exception;
    public void exec(SendEvent event) throws Exception;
    public void finished() throws Exception;
    public void doOnError() throws Exception;
    public StatisticsData getStats();
}
