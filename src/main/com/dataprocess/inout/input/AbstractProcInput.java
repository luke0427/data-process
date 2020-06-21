package com.ropeok.dataprocess.inout.input;

import com.ropeok.dataprocess.inout.ProcInput;
import com.ropeok.dataprocess.meta.IncreMeta;
import com.ropeok.dataprocess.meta.ProcInputOutputMeta;
import org.quartz.JobDetail;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.Date;
import java.util.Map;

public abstract class AbstractProcInput implements ProcInput{

    protected ProcInputOutputMeta procInputMeta;
    protected JdbcTemplate jdbcTemplate;
    protected JobDetail jobDetail;
    protected LocalDateTime startTime = null;
    protected LocalDateTime endTime = null;
    protected Date sd;
    protected Date ed;

    protected void preInit() {
        IncreMeta increMeta = procInputMeta.getIncreMeta();
        if(increMeta != null) {
            Map<String, Object> increMap = jdbcTemplate.queryForMap("SELECT START_TIME, END_TIME FROM JOB_INCRE_INFO WHERE JOB_GROUP = ? AND JOB_NAME = ?", new Object[]{jobDetail.getKey().getGroup(), jobDetail.getKey().getName()});
            this.startTime = increMap.get("START_TIME")!=null ? ((Timestamp)increMap.get("START_TIME")).toLocalDateTime() : null;
            this.endTime = increMap.get("END_TIME") != null ? ((Timestamp)increMap.get("END_TIME")).toLocalDateTime() : null;
        }
    }

    public Date getStartTime() {
        return sd;
    }

    public Date getEndTime() {
        return ed;
    }

    public JobDetail getJobDetail() {
        return jobDetail;
    }

    public void setJobDetail(JobDetail jobDetail) {
        this.jobDetail = jobDetail;
    }

    public JdbcTemplate getJdbcTemplate() {
        return jdbcTemplate;
    }

    public void setJdbcTemplate(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
    public void setProcInputMeta(ProcInputOutputMeta procInputMeta) {
        this.procInputMeta = procInputMeta;
    }

    @Override
    public ProcInputOutputMeta getProcInputMeta() {
        return this.procInputMeta;
    }
}
