package com.ropeok.dataprocess.config;

import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Preconditions;
import com.ropeok.dataprocess.job.BaseJob;
import com.ropeok.dataprocess.job.ProcJobContext;
import com.ropeok.dataprocess.meta.IncreMeta;
import com.ropeok.dataprocess.utils.Constants;
import com.ropeok.dataprocess.v2.analyzer.JobAnalyzerV2;
import com.ropeok.dataprocess.v2.job.BaseJobV2;
import com.ropeok.dataprocess.v2.meta.JobMetaV2;
import com.ropeok.dataprocess.v2.meta.StepMetaV2;
import org.apache.commons.lang3.StringUtils;
import org.quartz.*;
import org.quartz.spi.JobFactory;
import org.quartz.spi.TriggerFiredBundle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.AutowireCapableBeanFactory;
import org.springframework.beans.factory.config.PropertiesFactoryBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.quartz.SchedulerFactoryBean;
import org.springframework.scheduling.quartz.SpringBeanJobFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.*;
import java.util.*;

@Configuration
public class SchedulerConfig {

    public static final String QUARTZ_PROPERTIES_PATH = "/quartz.properties";
    public static final String SCHEDULE_PROPERTIES_PATH = "/schedule.xml";
    public static final String SCHEDULE_V2_PROPERTIES_PATH = "/schedule_v2.xml";
    private static final Logger LOGGER = LoggerFactory.getLogger(SchedulerConfig.class);

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Value("${procjob.init}")
    private String isInitProcJob;

    @Bean
    public JobFactory jobFactory(ApplicationContext applicationContext) {
        AutowiringSpringBeanJobFactory jobFactory = new AutowiringSpringBeanJobFactory();
        jobFactory.setApplicationContext(applicationContext);
        return jobFactory;
    }

    @Bean(name="schedulerFactoryBean")
    public SchedulerFactoryBean schedulerFactoryBean(JobFactory jobFactory) throws IOException {
        SchedulerFactoryBean factory = new SchedulerFactoryBean();
        factory.setAutoStartup(true);
        factory.setJobFactory(jobFactory);
        factory.setQuartzProperties(quartzProperties());
        return factory;
    }

    @Bean
    public Properties quartzProperties() throws IOException {
        PropertiesFactoryBean propertiesFactoryBean = new PropertiesFactoryBean();
        propertiesFactoryBean.setLocation(new ClassPathResource(QUARTZ_PROPERTIES_PATH));
        propertiesFactoryBean.afterPropertiesSet();
        return propertiesFactoryBean.getObject();
    }

    @Bean(name = "procJobContext2")
    public ProcJobContext initTaskV2(SchedulerFactoryBean schedulerFactoryBean) {
        if("true".equals(isInitProcJob)) {
            Scheduler scheduler = schedulerFactoryBean.getScheduler();
            //TODO: 可以在初始化任务前先清空所有任务，最好的处理方式是清理掉未配置的任务
            try {
                JobAnalyzerV2 jobAnalyzerV2 = new JobAnalyzerV2(new ClassPathResource(SCHEDULE_V2_PROPERTIES_PATH).getInputStream());
                Iterator<JobMetaV2> iterator = jobAnalyzerV2.getJobMetaV2s().iterator();
                while(iterator.hasNext()) {
                    JobMetaV2 jobMetaV2 = iterator.next();
                    String jsonParam = JSONObject.toJSONString(jobMetaV2);
                    System.out.println(jsonParam);
                    JobDataMap jobDataMap = new JobDataMap();
                    jobDataMap.put(BaseJobV2.JOB_PARAMS, jsonParam);
                    Class clazz = Class.forName(jobMetaV2.getClazz());
                    JobDetail jobDetail = JobBuilder.newJob(clazz).setJobData(jobDataMap).withDescription(jobMetaV2.getDesc()).withIdentity(jobMetaV2.getName(), jobMetaV2.getGroup()).storeDurably().build();

                    String cron = jobMetaV2.getCron();
                    Trigger trigger = null;
                    if(StringUtils.isNotBlank(cron)) {
                        trigger = TriggerBuilder.newTrigger().forJob(jobDetail).withSchedule(CronScheduleBuilder.cronSchedule(cron)).build();
                    }
                    if(scheduler.checkExists(JobKey.jobKey(jobMetaV2.getName(), jobMetaV2.getGroup()))) {
                        scheduler.deleteJob(JobKey.jobKey(jobMetaV2.getName(), jobMetaV2.getGroup()));
                        LOGGER.info("移除已注册V2任务{}", jobMetaV2.getKey());
                    }
                    //判断是否更新还是插入job_info_v2
                    String sql = "INSERT INTO " + Constants.TABLE_JOB_INFO + " (JOB_GROUP, JOB_NAME) VALUES ('%s', '%s') ON DUPLICATE KEY UPDATE JOB_GROUP='%s',JOB_NAME='%s'";
                    jdbcTemplate.execute(String.format(sql, jobMetaV2.getGroup(), jobMetaV2.getName(), jobMetaV2.getGroup(), jobMetaV2.getName()));
                    //判断初始步骤是否带增量，如果增量就将步骤配置的增量信息插入job_incre_info_v2
                    Iterator<StepMetaV2> stepIt = jobMetaV2.getStepMetaV2Map().values().iterator();
                    StepMetaV2 stepMetaV2 = null;
                    sql = "INSERT INTO " + Constants.TABLE_STEP_INFO + " (JOB_GROUP, JOB_NAME, JOB_STEP, INCRE) VALUES ('%s','%s','%s', %s) ON DUPLICATE KEY UPDATE INCRE=%s";
                    List<String> stepIds = new LinkedList<>();
                    while (stepIt.hasNext()) {
                        stepMetaV2 = stepIt.next();
                        stepIds.add(stepMetaV2.getId());
                        boolean incre = stepMetaV2.getBooleanProperty(Constants.INCRE);
                        jdbcTemplate.execute(String.format(sql, jobMetaV2.getGroup(), jobMetaV2.getName(), stepMetaV2.getId(), incre, incre));
                    }
                    //删除任务未配置的步骤
                    if(stepIds.size() > 0) {
                        sql = "DELETE FROM " + Constants.TABLE_STEP_INFO + " WHERE JOB_GROUP = '%s' AND JOB_NAME = '%s' AND JOB_STEP NOT IN (";
                        for(int i = 0, size = stepIds.size(); i < size; i++) {
                            sql += " '" + stepIds.get(i) + "'";
                            if(i < size-1){
                                sql += ",";
                            }
                        }
                        sql += ")";
                        LOGGER.info("Delete step sql:{}", sql);
                        jdbcTemplate.execute(String.format(sql, jobMetaV2.getGroup(), jobMetaV2.getName()));
                    }

                    if(trigger != null) {
                        scheduler.scheduleJob(jobDetail, trigger);
                    } else {
                        scheduler.addJob(jobDetail, true, true);
                    }
                    LOGGER.info("V2任务{}已注册", jobMetaV2.getKey());

                }
            } catch (IOException e) {
                LOGGER.error("Job任务解析异常,{}", e.getLocalizedMessage());
                e.printStackTrace();
                throw new RuntimeException(e.getLocalizedMessage());
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            } catch (SchedulerException e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    @Bean(name="procJobContext")
    public ProcJobContext initScheduleContext(SchedulerFactoryBean schedulerFactoryBean) {
        if("true".equals(isInitProcJob)) {

            Preconditions.checkNotNull(schedulerFactoryBean, "schedulerFactoryBean不能为空");
            Scheduler scheduler = schedulerFactoryBean.getScheduler();
            try {
                ProcJobContext procJobContext = new ProcJobContext(new ClassPathResource(SCHEDULE_PROPERTIES_PATH).getInputStream());
                Iterator<ProcJobContext.ProcJob> it = procJobContext.getProcJobs().iterator();

                while(it.hasNext()) {
                    ProcJobContext.ProcJob procJob = it.next();
                    String jsonParam = JSONObject.toJSONString(procJob);
                    JobDataMap jobDataMap = new JobDataMap();
                    jobDataMap.put(BaseJob.JOB_PARAMS, jsonParam);
                    JobDetail jobDetail = JobBuilder.newJob(procJob.getClazz()).setJobData(jobDataMap).withDescription(procJob.getDesc()).withIdentity(procJob.getName(), procJob.getGroup()).storeDurably().build();
                    String cron = procJob.getCron();
                    Trigger trigger = null;
                    if(StringUtils.isNotBlank(cron)) {
                        trigger = TriggerBuilder.newTrigger().forJob(jobDetail).withSchedule(CronScheduleBuilder.cronSchedule(procJob.getCron())).build();
                    }
                    if(scheduler.checkExists(JobKey.jobKey(procJob.getName(), procJob.getGroup()))) {
                        scheduler.deleteJob(JobKey.jobKey(procJob.getName(), procJob.getGroup()));
                        LOGGER.info("移除已注册任务{}", procJob.getKey());
                    }
                    IncreMeta increMeta = procJob.getProcInputMeta().getIncreMeta();
                    if(increMeta != null) {
                        //TODO : 先不支持固定值获取，后面再做修改
                        Map<String, Object> countMap = jdbcTemplate.queryForMap("SELECT COUNT(1) CT FROM JOB_INCRE_INFO WHERE JOB_GROUP = ? AND JOB_NAME = ?",new Object[]{procJob.getGroup(), procJob.getName()});
                        long ct = 0;
                        if(countMap.get("CT") instanceof BigDecimal) {
                            BigDecimal bd = (BigDecimal) countMap.get("CT");
                            ct = bd.longValue();
                        } else {
                            ct = (long) countMap.get("CT");
                        }
                        if(ct == 0) {
                            jdbcTemplate.execute("INSERT INTO JOB_INCRE_INFO(JOB_GROUP, JOB_NAME, COLUMN_NAME) VALUES ('"+ procJob.getGroup() + "','" + procJob.getName() + "','" + increMeta.getColumn() + "')");
                        }
                    } else {
                        jdbcTemplate.execute("DELETE FROM JOB_INCRE_INFO WHERE JOB_GROUP = '" + procJob.getGroup() + "' AND JOB_NAME = '" + procJob.getName() +"'");
                        jdbcTemplate.execute("INSERT INTO JOB_INCRE_INFO(JOB_GROUP, JOB_NAME) VALUES ('"+ procJob.getGroup() +"', '"+ procJob.getName() + "')");
                    }
                    if(trigger != null) {
                        scheduler.scheduleJob(jobDetail, trigger);
                    } else {
                        scheduler.addJob(jobDetail, true, true);
                    }
                    LOGGER.info("任务{}已注册", procJob.getKey());
                }
                return procJobContext;
            } catch (IOException e) {
                LOGGER.error("ProcJob任务解析异常,{}", e.getLocalizedMessage());
                e.printStackTrace();
                throw new RuntimeException(e.getLocalizedMessage());
            } catch (SchedulerException e) {
                LOGGER.error("ProcJob任务初始化异常,{}", e.getLocalizedMessage());
                e.printStackTrace();
                throw new RuntimeException(e.getLocalizedMessage());
            }
        }
        return null;
    }

    public static class AutowiringSpringBeanJobFactory extends SpringBeanJobFactory implements
            ApplicationContextAware {

        private transient AutowireCapableBeanFactory beanFactory;

        @Override
        public void setApplicationContext(final ApplicationContext context) {
            beanFactory = context.getAutowireCapableBeanFactory();
        }

        @Override
        protected Object createJobInstance(final TriggerFiredBundle bundle) throws Exception {
            final Object job = super.createJobInstance(bundle);
            beanFactory.autowireBean(job);
            return job;
        }
    }
}
