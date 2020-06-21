package com.ropeok.dataprocess.v2.core;

import com.ropeok.dataprocess.v2.component.IComponent;
import com.ropeok.dataprocess.v2.meta.StepMetaV2;

import java.util.Collection;
import java.util.Set;

/**
 * 执行步骤，管理component的生命周期，持有数据通道，负责数据的处理、生产等
 */
public interface IStep extends Runnable{
    public void init();
    public void stop();
    public void put(SendEvent event);
    public String getId();
    public String getName();
    public RunningStatus getStatus();
    public void addNextStep(IStep nextStep);
    public void finished(IComponent component);
    public StepMetaV2 getStepMeta();
    public boolean hasPrevStep();
    public boolean hasNextStep();
    public Collection<IStep> getPrevSteps();
    public Collection<IStep> getNextSteps();
    public void sendToNext(IComponent component, SendEvent event);
    public void sendToNextStep(IComponent component, SendEvent event, String nextStepId);
    public void receiveFinished(IStep prevStep);
    public void error(IComponent component);
    public StatisticsData getStats();
    public JobContext getContext();
}
