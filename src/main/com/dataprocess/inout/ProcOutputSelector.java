package com.ropeok.dataprocess.inout;

import com.ropeok.dataprocess.inout.output.AbstractProcOutput;
import com.ropeok.dataprocess.meta.ProcInputOutputMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class ProcOutputSelector {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProcOutputSelector.class);
    private final ExecutorService service;
    private final CountDownLatch workers;
    private int workerSize;
    private final List<ProcOutput> procOutputs;
    private int index = -1;


    public ProcOutputSelector(ProcInputOutputMeta procInputOutputMeta) {
        //初始化线程池数量
        this.workerSize = procInputOutputMeta.getPoolSize() <= 0 ? 1 : procInputOutputMeta.getPoolSize();
        LOGGER.info("初始化 {} 个输出线程", workerSize);
        this.workers = new CountDownLatch(workerSize);
        this.procOutputs = new ArrayList<>(workerSize);
        service = Executors.newFixedThreadPool(workerSize, new ThreadFactory() {
            private int count = 0;
            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "ProcOutputThread-" + (++count));
            }
        });

        try {
            for(int i = 0;i < workerSize; i++) {
                ProcOutput procOutput = procInputOutputMeta.getProcOutputType().getClazz().newInstance();
                procOutput.setProcOutputMeta(procInputOutputMeta);
                ((AbstractProcOutput)procOutput).setSingle(this.workers);
                procOutput.init();
                this.procOutputs.add(procOutput);
            }
            LOGGER.info("输出线程初始化完成", workerSize);
            this.start();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
    }

    public void put(Map<String, Object> data) {
        int d = getNextIndex();
//        LOGGER.info("获取到的输出位置: {}", d);
        procOutputs.get(d).put(data);
    }

    public synchronized int getNextIndex() {
        //访问共享变量获取当前访问的数字
        //访问共享变量并对共享变量+1，如果等于workerSize则置零
        return index = ++index >= workerSize ? 0 : index;
    }

    public void sendFinished() {
        LOGGER.info("发送抽取结束标志");
        for(int i = 0;i < workerSize; i++) {
            procOutputs.get(i).put(new HashMap<>());
        }
        try {
            LOGGER.info("等待输出线程工作结束");
            workers.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        LOGGER.info("输出线程工作结束");
    }

    public void start() {
        for(int i =0; i < workerSize; i++) {
            service.submit(procOutputs.get(i));
        }
    }

    public void stop() {
        this.service.shutdown();
        LOGGER.info("输出线程已关闭");
    }
}
