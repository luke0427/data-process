package com.ropeok.dataprocess.inout.output;

import com.ropeok.dataprocess.inout.ProcOutput;
import com.ropeok.dataprocess.meta.ProcInputOutputMeta;
import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;


public abstract class AbstractProcOutput implements ProcOutput {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractProcOutput.class);
    protected ProcInputOutputMeta procOutputMeta;
    private CountDownLatch isFinish;
    private ArrayBlockingQueue<Map<String, Object>> datas = new ArrayBlockingQueue<Map<String, Object>>(5000);
    protected StopWatch stopWatch = new StopWatch();

    @Override
    public void run() {
        stopWatch.start();
        try {
            while(true) {
                Map<String, Object> row = datas.take();
                if(row != null && row.size() > 0) {
                    exec(row);
                } else if(row != null && row.size() == 0) {
                    exec(row);
                    break;
                }
            }
        } catch (Exception e) {
            LOGGER.error("输出异常:{}", e.getLocalizedMessage());
            e.printStackTrace();
        } finally {
            try {
                finished();
                this.isFinish.countDown();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        LOGGER.info("当前任务已完成");
        stopWatch.stop();
    }

    @Override
    public void put(Map<String, Object> data) {
        try {
            this.datas.put(data);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    protected abstract void exec(Map<String, Object> row) throws Exception;

    @Override
    public ProcInputOutputMeta getProcOutputMeta() {
        return procOutputMeta;
    }

    @Override
    public void setProcOutputMeta(ProcInputOutputMeta procOutputMeta) {
        this.procOutputMeta = procOutputMeta;
    }

    public void setSingle(CountDownLatch single) {
        this.isFinish = single;
    }
}
