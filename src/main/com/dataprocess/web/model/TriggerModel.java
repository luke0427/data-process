package com.ropeok.dataprocess.web.model;

public class TriggerModel {
    private String triggerName;
    private String triggerGroup;
    private String jobName;
    private String jobGroup;
    private String triggerState;
    private String prevFireTime;
    private String nextFireTime;
    private String cronExpression;
    private String jobStartTime;
    private String jobEndTime;
    private long costTime;
    private String jobExecResult;
    private long procRow;
    private boolean isIncre;

    public boolean getIsIncre() {
        return isIncre;
    }

    public void setIsIncre(boolean incre) {
        isIncre = incre;
    }

    public long getProcRow() {
        return procRow;
    }

    public void setProcRow(long procRow) {
        this.procRow = procRow;
    }

    public String getJobExecResult() {
        return jobExecResult;
    }

    public void setJobExecResult(String jobExecResult) {
        this.jobExecResult = jobExecResult;
    }

    public String getJobStartTime() {
        return jobStartTime;
    }

    public void setJobStartTime(String jobStartTime) {
        this.jobStartTime = jobStartTime;
    }

    public String getJobEndTime() {
        return jobEndTime;
    }

    public void setJobEndTime(String jobEndTime) {
        this.jobEndTime = jobEndTime;
    }

    public long getCostTime() {
        return costTime;
    }

    public void setCostTime(long costTime) {
        this.costTime = costTime;
    }

    public String getCronExpression() {
        return cronExpression;
    }

    public void setCronExpression(String cronExpression) {
        this.cronExpression = cronExpression;
    }

    public String getTriggerName() {
        return triggerName;
    }

    public void setTriggerName(String triggerName) {
        this.triggerName = triggerName;
    }

    public String getTriggerGroup() {
        return triggerGroup;
    }

    public void setTriggerGroup(String triggerGroup) {
        this.triggerGroup = triggerGroup;
    }

    public String getJobName() {
        return jobName;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    public String getJobGroup() {
        return jobGroup;
    }

    public void setJobGroup(String jobGroup) {
        this.jobGroup = jobGroup;
    }

    public String getTriggerState() {
        return triggerState;
    }

    public void setTriggerState(String triggerState) {
        this.triggerState = triggerState;
    }

    public String getPrevFireTime() {
        return prevFireTime;
    }

    public void setPrevFireTime(String prevFireTime) {
        this.prevFireTime = prevFireTime;
    }

    public String getNextFireTime() {
        return nextFireTime;
    }

    public void setNextFireTime(String nextFireTime) {
        this.nextFireTime = nextFireTime;
    }
}
