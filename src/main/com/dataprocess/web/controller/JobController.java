package com.ropeok.dataprocess.web.controller;

import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Preconditions;
import com.ropeok.dataprocess.inout.input.ParamProcInput;
import com.ropeok.dataprocess.job.BaseJob;
import com.ropeok.dataprocess.job.ProcJobContext;
import com.ropeok.dataprocess.meta.ProcInputOutputMeta;
import com.ropeok.dataprocess.web.model.TriggerModel;
import org.quartz.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.scheduling.quartz.SchedulerFactoryBean;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.*;
import java.util.Calendar;
import java.util.concurrent.ArrayBlockingQueue;

@Controller
@EnableAutoConfiguration
public class JobController {

    @Autowired
    private SchedulerFactoryBean schedulerFactoryBean;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private ProcJobContext procJobContext;

    private static final SimpleDateFormat DATE_FORMATTER = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private static final Logger LOGGER = LoggerFactory.getLogger(JobController.class);
    private Map<String, MiniBatch> miniBatchMap = new HashMap<>();

    /*public JobController() {
        miniBatchMap = new HashMap<>();
        Set<String> keys = procJobContext.getTriggerJobKeys();
        Scheduler scheduler = schedulerFactoryBean.getScheduler();
        try {
            for(String key : keys) {
                JobDetail jobDetail = scheduler.getJobDetail(procJobContext.getTriggerJobKey(key));
                miniBatchMap.put(key, new MiniJobContext(jobDetail, 10000, scheduler));
            }
        } catch (SchedulerException e) {
            e.printStackTrace();
        }
    }*/

    @RequestMapping("/jobList")
    public String jobList(ModelMap modelMap) {
        Scheduler scheduler = schedulerFactoryBean.getScheduler();
        List<TriggerModel> triggers = jdbcTemplate.query(
                "(SELECT\n" +
                    "	t3.JOB_NAME,\n" +
                    "	t3.JOB_GROUP,\n" +
                    "	t.TRIGGER_NAME,\n" +
                    "	t.TRIGGER_GROUP,\n" +
                    "	t.TRIGGER_STATE,\n" +
                    "	t2.CRON_EXPRESSION,\n" +
                    "	t.PREV_FIRE_TIME,\n" +
                    "	t.NEXT_FIRE_TIME,\n" +
                    "	t3.JOB_START_TIME,\n" +
                    "	t3.JOB_END_TIME,\n" +
                    "	t3.JOB_EXEC_RESULT,\n" +
                    "	t3.PROC_ROW\n" +
                    "FROM\n" +
                    "	JOB_INFO_V2 t3\n" +
                    "LEFT JOIN QRTZ_TRIGGERS t ON t3.job_group = t.job_group\n" +
                    "AND t3.job_name = t.job_name\n" +
                    "LEFT JOIN QRTZ_CRON_TRIGGERS t2 ON t2.trigger_name = t.trigger_name\n" +
                    "AND t2.trigger_group = t.trigger_group\n" +
                    "ORDER BY\n" +
                    "	t.JOB_GROUP,\n" +
                    "	t.JOB_NAME,\n" +
                    "	t.NEXT_FIRE_TIME)" +
                    "UNION ALL " +
                    "(SELECT\n" +
                        "	t3.JOB_NAME,\n" +
                        "	t3.JOB_GROUP,\n" +
                        "	t.TRIGGER_NAME,\n" +
                        "	t.TRIGGER_GROUP,\n" +
                        "	t.TRIGGER_STATE,\n" +
                        "	t2.CRON_EXPRESSION,\n" +
                        "	t.PREV_FIRE_TIME,\n" +
                        "	t.NEXT_FIRE_TIME,\n" +
                        "	t3.JOB_START_TIME,\n" +
                        "	t3.JOB_END_TIME,\n" +
                        "	t3.JOB_EXEC_RESULT,\n" +
                        "	t3.PROC_ROW\n" +
                        "FROM\n" +
                        "	JOB_INCRE_INFO t3\n" +
                        "LEFT JOIN QRTZ_TRIGGERS t ON t3.job_group = t.job_group\n" +
                        "AND t3.job_name = t.job_name\n" +
                        "LEFT JOIN QRTZ_CRON_TRIGGERS t2 ON t2.trigger_name = t.trigger_name\n" +
                        "AND t2.trigger_group = t.trigger_group\n" +
                        "ORDER BY\n" +
                        "	t.JOB_GROUP,\n" +
                        "	t.JOB_NAME,\n" +
                        "	t.NEXT_FIRE_TIME)"
, new RowMapper<TriggerModel>() {
            @Override
            public TriggerModel mapRow(ResultSet resultSet, int i) throws SQLException {
                TriggerModel tm = new TriggerModel();
                tm.setJobName(resultSet.getString("JOB_NAME"));
                tm.setJobGroup(resultSet.getString("JOB_GROUP"));
                tm.setTriggerName(resultSet.getString("TRIGGER_NAME"));
                tm.setTriggerGroup(resultSet.getString("TRIGGER_GROUP"));
                tm.setTriggerState(resultSet.getString("TRIGGER_STATE"));
                tm.setCronExpression(resultSet.getString("CRON_EXPRESSION"));
                BigDecimal pft = resultSet.getBigDecimal("PREV_FIRE_TIME");
                BigDecimal nft = resultSet.getBigDecimal("NEXT_FIRE_TIME");
                if(tm.getJobName() != null) {
                    try {
                        JobDetail jobDetail = scheduler.getJobDetail(JobKey.jobKey(tm.getJobName(), tm.getJobGroup()));
                        if(jobDetail != null) {
                            ProcInputOutputMeta meta = JSONObject.parseObject(jobDetail.getJobDataMap().getString(BaseJob.JOB_PARAMS)).getObject("procInputMeta", ProcInputOutputMeta.class);
                            if (meta != null && meta.getIncreMeta() != null) {
                                tm.setIsIncre(true);
                            }
                        }
                    } catch (SchedulerException e) {
                        e.printStackTrace();
                    }
                }
                if(pft != null && nft != null) {
                    Calendar c = Calendar.getInstance();
                    c.setTimeInMillis(pft.longValue());
                    tm.setPrevFireTime(DATE_FORMATTER.format(c.getTime()));
                    c.setTimeInMillis(nft.longValue());
                    tm.setNextFireTime(DATE_FORMATTER.format(c.getTime()));
                }
                Timestamp jobStart = resultSet.getTimestamp("JOB_START_TIME");
                Timestamp jobEnd = resultSet.getTimestamp("JOB_END_TIME");
                tm.setJobStartTime(jobStart != null ? jobStart.toLocalDateTime().toString() : null);
                tm.setJobEndTime(jobEnd != null ? jobEnd.toLocalDateTime().toString() : null);
                if(jobStart != null && jobEnd != null) {
                    tm.setCostTime(Duration.between(jobStart.toLocalDateTime(), jobEnd.toLocalDateTime()).toMinutes());
                }
                tm.setJobExecResult(resultSet.getString("JOB_EXEC_RESULT"));
                tm.setProcRow(resultSet.getLong("PROC_ROW"));
                return tm;
            }
        });
        modelMap.put("jobList", triggers);
        return "jobList";
    }

    @RequestMapping(value="/modifyJobDetail", method = {RequestMethod.POST})
    @ResponseBody
    public String modifyJobDetail(@RequestBody  Map<String, Object> params) {
        String triggerName = (String) params.get("triggerName");
        String triggerGroup = (String) params.get("triggerGroup");
        String cronExp = (String) params.get("cronExpression");
        Preconditions.checkNotNull(triggerName);
        Preconditions.checkNotNull(triggerGroup);
        Preconditions.checkNotNull(cronExp);
        Scheduler scheduler = schedulerFactoryBean.getScheduler();
        try {
            TriggerKey triggerKey = TriggerKey.triggerKey(triggerName, triggerGroup);
            CronTrigger cronTrigger = (CronTrigger) scheduler.getTrigger(triggerKey);
            if(cronTrigger != null) {
                String oldCron = cronTrigger.getCronExpression();
                if(!oldCron.equals(cronExp)) {
                    TriggerBuilder triggerBuilder = TriggerBuilder.newTrigger();
                    triggerBuilder.withIdentity(triggerName, triggerGroup);
                    triggerBuilder.startNow();
                    triggerBuilder.withSchedule(CronScheduleBuilder.cronSchedule(cronExp));
                    cronTrigger = (CronTrigger) triggerBuilder.build();
                    scheduler.rescheduleJob(triggerKey, cronTrigger);
                }
            }
        } catch (SchedulerException e) {
            e.printStackTrace();
            return "error: " + e.getLocalizedMessage();
        }
        return "succ";
    }

    @RequestMapping("/triggerJob")
    @ResponseBody
    public String triggerJob(@RequestBody Map<String, Object> params) {
        LOGGER.info("params={}", params);
        String key = (String) params.get("KEY");
        Preconditions.checkNotNull(key, "key is not null");
        Preconditions.checkNotNull(params.get(ParamProcInput.DATA_KEY), "data is not null");
        Scheduler scheduler = schedulerFactoryBean.getScheduler();
        try {
            JobDetail jobDetail = scheduler.getJobDetail(procJobContext.getTriggerJobKey(key));
            Preconditions.checkNotNull(jobDetail, "job is null");
            MiniBatch miniBatch = miniBatchMap.get(key);
            if(miniBatch == null) {
                miniBatch = new MiniBatch(jobDetail, 5000, scheduler);
                miniBatchMap.put(key, miniBatch);
            }
            miniBatch.putDatas((List<Map<String, Object>>) params.get(ParamProcInput.DATA_KEY));
        } catch (SchedulerException e) {
            e.printStackTrace();
            return "error";
        }
        /*String startTime = params.get(Constants.START_TIME);
        String endTime = params.get(Constants.END_TIME);
        Preconditions.checkNotNull(key, "key不能为空");
        try {
            JobKey jobKey = procJobContext.getTriggerJobKey(key);
            Preconditions.checkNotNull(jobKey, "wrong key");
            Scheduler scheduler = schedulerFactoryBean.getScheduler();
            List<JobExecutionContext> runningJobs = scheduler.getCurrentlyExecutingJobs();
            String t_key = null;
            for(JobExecutionContext ctx : runningJobs) {
                t_key = ctx.getJobDetail().getKey().toString();
                if(t_key.equals(jobKey.toString())) {//会漏数据要缓存1个最小时间
                    LOGGER.info("jobKey={} is running", jobKey.toString());
                    return "succ-running";
                }
            }
            JobDetail jobDetail = scheduler.getJobDetail(jobKey);
            if(StringUtils.isNotBlank(startTime)) {
                jobDetail.getJobDataMap().put(Constants.START_TIME, startTime);
            }
            if(StringUtils.isNotBlank(endTime)) {
                jobDetail.getJobDataMap().put(Constants.END_TIME, endTime);
            }
            scheduler.addJob(jobDetail, true, true);
            scheduler.triggerJob(jobKey);
            LOGGER.info("触发任务：{}", jobKey);
        } catch (SchedulerException e) {
            e.printStackTrace();
            return "error";
        }*/
        return "succ";
    }


    private static class MiniBatch {
        private final Timer timer;
        private final MiniBatchJob job;

        public MiniBatch(JobDetail jobDetail, long period, Scheduler scheduler) {
            this.timer = new Timer();
            this.job = new MiniBatchJob(jobDetail, scheduler);
            timer.scheduleAtFixedRate(job, 0, period);
            timer.cancel();
        }

        public void putDatas(List<Map<String, Object>> data) {
            job.putDatas(data);
        }

    }

    private static class MiniBatchJob extends TimerTask {
        private final ArrayBlockingQueue<Map<String, Object>> datas;
        private final List<Map<String, Object>> takeDatas;
        private final JobDetail jobDetail;
        private final Scheduler scheduler;

        public MiniBatchJob(JobDetail jobDetail, Scheduler scheduler) {
            datas = new ArrayBlockingQueue<Map<String, Object>>(500);
            takeDatas = new ArrayList<>(500);
            this.jobDetail = jobDetail;
            this.scheduler = scheduler;
        }

        @Override
        public void run() {
            int size = datas.size();
            try {
                while(size > 0) {
//                    LOGGER.info("task running, size={}", size);
                    takeDatas.add(datas.take());
                    size --;
                }
                if(takeDatas.size() > 0) {
                    try {
                        jobDetail.getJobDataMap().put(ParamProcInput.DATA_KEY, JSONObject.toJSONString(takeDatas));
                        scheduler.addJob(jobDetail, true, true);
                        scheduler.triggerJob(jobDetail.getKey());
                        LOGGER.info("触发任务：{}, data={}", jobDetail.getKey(), takeDatas);
                        takeDatas.clear();
                    } catch (SchedulerException e) {
                        e.printStackTrace();
                    }
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        public void putDatas(List<Map<String, Object>> dataList) {
            try {
                for(Map<String, Object> data : dataList) {
                    if(data != null) {
                        datas.put(data);
                    } else {
                        LOGGER.warn("数据为空:{}", data);
                    }
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
