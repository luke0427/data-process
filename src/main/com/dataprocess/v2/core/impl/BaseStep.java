package com.ropeok.dataprocess.v2.core.impl;

import com.google.common.base.Preconditions;
import com.ropeok.dataprocess.common.CommonThreadFactory;
import com.ropeok.dataprocess.common.JobRunException;
import com.ropeok.dataprocess.utils.CloneUtils;
import com.ropeok.dataprocess.utils.Constants;
import com.ropeok.dataprocess.v2.component.AbstractComponent;
import com.ropeok.dataprocess.v2.component.IComponent;
import com.ropeok.dataprocess.v2.core.*;
import com.ropeok.dataprocess.v2.meta.StepMetaV2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class BaseStep implements IStep {

    private static final Logger LOGGER = LoggerFactory.getLogger(BaseStep.class);
    private final StepMetaV2 stepMetaV2;
    private final JobContext jobContext;
    private RunningStatus stepStatus = RunningStatus.INIT;
    private final ExecutorService dataService;
    private final CountDownLatch componentCount;
    private final Map<Integer, IComponent> componentMap;
    private final Map<Integer, ArrayBlockingQueue> dataMap;
    private final Map<String, IStep> prevStepMap = new HashMap<>();
    private final Map<String, IStep> nextStepMap = new HashMap<>();
    private int threadNums;
    private boolean hasPrevStep = false;
    private boolean hasNextStep = false;
    private static final Integer CONTAINER_SIZE = 2000;
    private int index = -1;
    private final Object indexLock = new Object();
    private final StatisticsData stats;

    public BaseStep(StepMetaV2 stepMetaV2, JobContext jobContext) {
        this.stepMetaV2 = stepMetaV2;
        this.jobContext = jobContext;
        String strThreadNums = stepMetaV2.getOrDefaultStringProperty(Constants.THREAD_NUMS, "1");
        this.threadNums = Integer.parseInt(strThreadNums);
        LOGGER.info("[{}]初始化线程数={}", stepMetaV2.getName(), this.threadNums);
        dataService = Executors.newFixedThreadPool(threadNums, new CommonThreadFactory(stepMetaV2.getName()));
        componentCount = new CountDownLatch(threadNums);
        componentMap = new HashMap<>();
        dataMap = new HashMap<>();
        try {
            for(int i = 0; i < threadNums; i++) {
                Constructor<?> constructor = Class.forName(stepMetaV2.getClazz()).getConstructor();
                ArrayBlockingQueue<SendEvent> events = new ArrayBlockingQueue<SendEvent>(CONTAINER_SIZE);
                IComponent component = (IComponent) constructor.newInstance();
                AbstractComponent aComp = (AbstractComponent) component;
                aComp.setId(i);
                aComp.setStep(this);
                aComp.setEvents(events);
                aComp.init();
                componentMap.put(i, component);
                dataMap.put(i, events);
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new JobRunException("步骤[" + stepMetaV2.getName() + "]初始化异常:" + e.getLocalizedMessage());
        }
        stats = new StatisticsData(this.getName());
    }

    @Override
    public void run() {
        stats.start();
        stepStatus = RunningStatus.RUNNING;
        LOGGER.info("启动[{}]组件", stepMetaV2.getName());
        try {
            for(int i = 0; i < this.threadNums; i++) {
                dataService.submit(componentMap.get(i));
            }
            componentCount.await();
            stop();
            stepStatus = RunningStatus.FINISHED;
            //通知下一步当前步骤已经完成
            for(IStep step : getNextSteps()) {
                step.receiveFinished(this);
            }
            jobContext.finished(this);
        } catch (InterruptedException ie) {
            LOGGER.info("[{}]被打扰了", getName());
        } catch (Exception e) {
            stepStatus = RunningStatus.ERROR;
            e.printStackTrace();
            jobContext.error(this);
        } finally {
            LOGGER.info("步骤[{}]统计信息：\n{}", getName(), stats.toString());
        }
    }

    @Override
    public void init() {
        //初始化上一步下一步
        initStepMap(stepMetaV2.getNextStepMetaV2(), nextStepMap, 2);
        initStepMap(stepMetaV2.getPrevStepMetaV2(), prevStepMap, 1);
    }

    private void initStepMap(Set<String> steps, Map<String, IStep> stepMap, int flag) {
        if(steps != null && steps.size() > 0) {
            if(flag == 1) {
                this.hasPrevStep = true;
            } else {
                this.hasNextStep = true;
            }
            Iterator<String> iterator = steps.iterator();
            while(iterator.hasNext()) {
                IStep nextStep = jobContext.getStep(iterator.next());
                stepMap.put(nextStep.getId(), nextStep);
            }
        }
    }

    @Override
    public void stop() {
        LOGGER.info("正在停止步骤[{}]", stepMetaV2.getName());
        dataService.shutdownNow();
    }

    @Override
    public void put(SendEvent event) {
        try {
            dataMap.get(getNextIndex()).put(event);
        } catch (InterruptedException e) {
        }
    }

    public int getNextIndex() {
        //访问共享变量获取当前访问的数字
        //访问共享变量并对共享变量+1，如果等于workerSize则置零
        synchronized (indexLock) {
            return index = ++index >= threadNums ? 0 : index;
        }
    }

    @Override
    public String getId() {
        return stepMetaV2.getId();
    }

    @Override
    public String getName() {
        return stepMetaV2.getName();
    }

    @Override
    public RunningStatus getStatus() {
        return stepStatus;
    }

    @Override
    public void addNextStep(IStep nextStep) {
        this.nextStepMap.put(nextStep.getId(), nextStep);
    }

    @Override
    public void finished(IComponent component) {
        LOGGER.info("组件[{}]汇报完成", component.getName());
        stats.addStat(component.getStats());
        componentCount.countDown();
    }

    @Override
    public StepMetaV2 getStepMeta() {
        return this.stepMetaV2;
    }

    @Override
    public boolean hasPrevStep() {
        return this.hasPrevStep;
    }

    @Override
    public boolean hasNextStep() {
        return this.hasNextStep;
    }

    @Override
    public Collection<IStep> getPrevSteps() {
        return this.prevStepMap.values();
    }

    @Override
    public Collection<IStep> getNextSteps() {
        return this.nextStepMap.values();
    }

    @Override
    public void sendToNext(IComponent component, SendEvent event) {
//        SendEvent copyEvent = CloneUtils.clone(event);
        if(getNextSteps().size() == 1) {
            getNextSteps().iterator().next().put(event);
        } else {
            for(IStep step : getNextSteps()) {
                //TODO: 复制/分发数据(已完成数据复制下发，如果需要根据条件判断分发规则需要另外处理)
                step.put(CloneUtils.clone(event));
            }
        }
    }

    @Override
    public void sendToNextStep(IComponent component, SendEvent event, String nextStepId) {
        IStep nextStep = nextStepMap.get(nextStepId);
        Preconditions.checkNotNull(nextStep, "下一步骤为空");
        nextStep.put(event);
    }

    @Override
    public void receiveFinished(IStep prevStep) {
        //接收上一步的完成标志
        SendEvent finishEvent = new SendEvent(SendEvent.EventStatus.FINISHED);
        Iterator<ArrayBlockingQueue> iterator = dataMap.values().iterator();
        try {
            while(iterator.hasNext()) {
                iterator.next().put(finishEvent);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    @Override
    public synchronized void error(IComponent component) {
        LOGGER.info("组件[{}]上报执行异常", component.getName());
        stepStatus = RunningStatus.ERROR;
        jobContext.error(this);
    }

    @Override
    public StatisticsData getStats() {
        return this.stats;
    }

    @Override
    public JobContext getContext() {
        return jobContext;
    }

    private void sendFinishToNext() {
        SendEvent finishEvent = new SendEvent(SendEvent.EventStatus.FINISHED);
        for(IStep step : getNextSteps()) {
            step.put(finishEvent);
        }
    }

}
