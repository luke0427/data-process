package com.ropeok.dataprocess.v2.job;

import com.ropeok.dataprocess.v2.meta.JobMetaV2;
import org.quartz.Job;

public interface BaseJobV2 extends Job {
    public static final String JOB_PARAMS = "params";
    public JobMetaV2 getJobMetaV2();
    public void init() throws Exception;
    public void exec() throws Exception;
    public void finished() throws Exception;
    public void doOnError() throws Exception;
}
