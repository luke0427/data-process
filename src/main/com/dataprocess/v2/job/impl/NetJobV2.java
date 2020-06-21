package com.ropeok.dataprocess.v2.job.impl;

import com.ropeok.dataprocess.v2.core.JobContext;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.PersistJobDataAfterExecution;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@PersistJobDataAfterExecution
@DisallowConcurrentExecution
public class NetJobV2 extends AbstractJobV2{

    private static final Logger LOGGER = LoggerFactory.getLogger(NetJobV2.class);
    private JobContext jobContext;
    @Override
    public void init() throws Exception {
        /*
        JobContext: job执行的上下文，包含所有step，
                    管理step生命周期，负责step的启动和关闭
             Step:  执行步骤，管理component的生命周期，持有数据通道，
                    负责数据的处理、生产等
        Component:  具体的工作组件，实现具体数据的生产、处理等操作。
        */
        jobContext = new JobContext(jobMetaV2, jdbcTemplate);
    }

    @Override
    public void exec() throws Exception {
        jobContext.start();
    }

    @Override
    public void finished() throws Exception {
        if(jobContext != null) {
            jobContext.stop();
        }
    }

    @Override
    public void doOnError() throws Exception {
        LOGGER.info("任务[{}]执行错误操作", jobMetaV2.getKey());
    }
}
