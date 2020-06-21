package com.ropeok.dataprocess.v2.core;

import com.ropeok.dataprocess.common.CommonThreadFactory;
import com.ropeok.dataprocess.common.JobRunException;
import com.ropeok.dataprocess.v2.core.impl.BaseStep;
import com.ropeok.dataprocess.v2.meta.JobMetaV2;
import com.ropeok.dataprocess.v2.meta.StepMetaV2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * job执行的上下文，包含所有step，管理step生命周期，负责step的启动和关闭
 */
public class JobContext {

    private static final Logger LOGGER = LoggerFactory.getLogger(JobContext.class);
    private JobMetaV2 jobMetaV2;
    private Map<String, StepMetaV2> stepMetaV2Map;
    private Map<String, IStep> stepMap = new HashMap<>();
    private CountDownLatch mainLatch;
    private ExecutorService service;
    private RunningStatus jobStatus = RunningStatus.INIT;
    private StatisticsData stats;
    private JdbcTemplate jdbcTemplate;

    public JobContext(JobMetaV2 jobMetaV2, JdbcTemplate jdbcTemplate) {
        this.jobMetaV2 = jobMetaV2;
        this.stepMetaV2Map = jobMetaV2.getStepMetaV2Map();
        this.jdbcTemplate = jdbcTemplate;
        init();
        service = Executors.newFixedThreadPool(this.stepMetaV2Map.size(), new CommonThreadFactory(jobMetaV2.getKey()));
        mainLatch = new CountDownLatch(this.stepMetaV2Map.size());
        stats = new StatisticsData(this.jobMetaV2.getName());
    }

    private void init() {
        Iterator<StepMetaV2> iterator = stepMetaV2Map.values().iterator();
        while(iterator.hasNext()) {
            StepMetaV2 stepMetaV2 = iterator.next();
            IStep step = new BaseStep(stepMetaV2, this);
            stepMap.put(step.getId(), step);
        }
        Iterator<IStep> stepIterator = stepMap.values().iterator();
        while(stepIterator.hasNext()) {
            stepIterator.next().init();
        }
    }

    public void start() {
        stats.start();
        LOGGER.info("[{}]任务开始执行", jobMetaV2.getKey());
        jobStatus = RunningStatus.RUNNING;
        //启动所有Step进行生产
        Iterator<IStep> iterator = this.stepMap.values().iterator();
        while(iterator.hasNext()) {
            service.submit(iterator.next());
        }
        //等待任务完成或者出错
        try {
            mainLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        if(jobStatus == RunningStatus.ERROR) {
            throw new JobRunException("[" + jobMetaV2.getKey() + "]任务执行异常");
        }
        LOGGER.info("任务[{}]统计信息：\n{}", jobMetaV2.getKey(), stats.toString());
    }

    public void stop() {
        LOGGER.info("停止任务[{}]", jobMetaV2.getKey());
        service.shutdownNow();
    }

    public void finished(IStep step) {
        LOGGER.info("步骤[{}]汇报完成", step.getName());
        stats.addStat(step.getStats());
        mainLatch.countDown();
        //TODO: 未设置JobContext的完成状态，可以设置一个计数器设置状态
    }

    public synchronized void error(IStep step) {
        LOGGER.info("步骤[{}]发生异常", step.getName());
        jobStatus = RunningStatus.ERROR;
        Iterator<IStep> iterator = stepMap.values().iterator();
        while(iterator.hasNext()) {
            IStep st = iterator.next();
            if(st.getStatus() != RunningStatus.FINISHED || st.getStatus() != RunningStatus.ERROR) {
                st.stop();
                mainLatch.countDown();
                LOGGER.info("强制结束步骤[{}]", st.getName());
            }
        }
        LOGGER.info("结束所有工作");
    }

    public String getGroup() {
        return jobMetaV2.getGroup();
    }

    public String getName() {
        return jobMetaV2.getName();
    }

    public String getKey() {
        return jobMetaV2.getKey();
    }

    public IStep getStep(String id) {
        return stepMap.get(id);
    }

    public StatisticsData getStats() {
        return stats;
    }

    public JdbcTemplate getJdbcTemplate() {
        return jdbcTemplate;
    }

}
