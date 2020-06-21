package com.ropeok.dataprocess.v2.job.impl;

import com.alibaba.fastjson.JSONObject;
import com.ropeok.dataprocess.utils.Constants;
import com.ropeok.dataprocess.v2.core.StatisticsData;
import com.ropeok.dataprocess.v2.job.BaseJobV2;
import com.ropeok.dataprocess.v2.meta.JobMetaV2;
import org.apache.commons.lang3.time.StopWatch;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

public abstract class AbstractJobV2 implements BaseJobV2 {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractJobV2.class);
    protected JobMetaV2 jobMetaV2;
    @Autowired
    protected JdbcTemplate jdbcTemplate;
    protected StopWatch stopWatch = new StopWatch();
    /**
     * 处理条数
     */
    protected long procRow = 0;

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        String execResult = "1";
        stopWatch.start();
        try {
            parseJob(context);
            //TODO:这里可以往表里面设置一个正在执行的状态标识
            jdbcTemplate.update("UPDATE " + Constants.TABLE_JOB_INFO + " SET JOB_START_TIME = ? WHERE JOB_GROUP = ? AND JOB_NAME = ?", new Object[]{new java.util.Date(), jobMetaV2.getGroup(), jobMetaV2.getName()});
            LOGGER.info("开始初始化任务[{}]", jobMetaV2.getKey());
            init();
            LOGGER.info("任务[{}]初始化完成，共耗时:{}ms", jobMetaV2.getKey(), stopWatch.getTime());
            exec();
            afterExec();
        } catch (Exception e) {
            execResult = "0";
            LOGGER.error("[{}]执行失败,异常信息:{}", jobMetaV2.getKey(), e.getLocalizedMessage());
            e.printStackTrace();
            try {
                doOnError();
            } catch (Exception e1) {
                e1.printStackTrace();
            }
//            throw new RuntimeException(jobMetaV2.getKey() + ",执行失败");
        } finally {
            try {
                LOGGER.info("开始执行[{}]收尾工作", jobMetaV2.getKey());
                finished();
                LOGGER.info("[{}]收尾工作执行完成", jobMetaV2.getKey());
                if(execResult.equals("0")) {
                    jdbcTemplate.update("UPDATE " + Constants.TABLE_JOB_INFO + " SET JOB_EXEC_RESULT = ?, PROC_ROW = ? WHERE JOB_GROUP = ? AND JOB_NAME = ?", new Object[]{execResult, procRow, jobMetaV2.getGroup(), jobMetaV2.getName()});
                } else {
                    jdbcTemplate.update("UPDATE " + Constants.TABLE_JOB_INFO + " SET JOB_END_TIME = ?, JOB_EXEC_RESULT = ?, PROC_ROW = ? WHERE JOB_GROUP = ? AND JOB_NAME = ?", new Object[]{new java.util.Date(), execResult, procRow, jobMetaV2.getGroup(), jobMetaV2.getName()});
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        LOGGER.info("任务[{}]已完成，耗时{}ms", jobMetaV2.getKey(), stopWatch.getTime());
        stopWatch.stop();
    }

    protected void parseJob(JobExecutionContext context) {
        String params = context.getMergedJobDataMap().getString(BaseJobV2.JOB_PARAMS);
        LOGGER.info("{}", params);
        this.jobMetaV2 = JSONObject.parseObject(params, JobMetaV2.class);
    }

    protected void afterExec() throws Exception{
    }

    @Override
    public JobMetaV2 getJobMetaV2() {
        return jobMetaV2;
    }

    public void setJobMetaV2(JobMetaV2 jobMetaV2) {
        this.jobMetaV2 = jobMetaV2;
    }
}
