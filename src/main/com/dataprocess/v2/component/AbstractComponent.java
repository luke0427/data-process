package com.ropeok.dataprocess.v2.component;

import com.ropeok.dataprocess.utils.Constants;
import com.ropeok.dataprocess.v2.core.IStep;
import com.ropeok.dataprocess.v2.core.SendEvent;
import com.ropeok.dataprocess.v2.core.StatisticsData;
import com.ropeok.dataprocess.v2.meta.StepMetaV2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class AbstractComponent implements IComponent {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractComponent.class);
    protected IStep step;
    private int id;
    protected ArrayBlockingQueue<SendEvent> events;
    private final AtomicInteger prevFinished = new AtomicInteger();
    protected StatisticsData stats;
    protected Date maxUpdateDate;

    @Override
    public void run() {
        stats = new StatisticsData(getName());
        stats.start();
        try {
            LOGGER.info("[{}]组件开始执行, hasPrevStep={}, hasNextStep={}", getName(), step.hasPrevStep(), step.hasNextStep());
            if(step.hasPrevStep()) {//TODO: 可能在多个输入的时候这样写会报错，可以通过判断上一步的完成数量来处理(已完成)
                while (true) {
                    SendEvent event = events.take();
                    //上一步可能有多个，只有当所有的上一步都完成了才能结束
                    if (event.getEventStatus() == SendEvent.EventStatus.DATA) {
                        stats.increRow();
                        exec(event);
                    } else if (event.getEventStatus() == SendEvent.EventStatus.FINISHED && (step.getPrevSteps().size() == 1 || prevFinished.incrementAndGet() == step.getPrevSteps().size())) {
                        beforeFinished();
//                        LOGGER.info("breka: prevStepNums={}, prevFinished={}", step.getPrevSteps().size(), prevFinished.intValue());//暂时取消打印步骤信息
                        break;
                    }
                }
            } else {
                exec(null);
            }
            maxUpdateDate = maxUpdateDate != null ? maxUpdateDate : new Date();
            step.getContext().getJdbcTemplate().update("UPDATE " + Constants.TABLE_STEP_INFO + " SET STEP_END_TIME = ?, PROC_ROW = ? WHERE JOB_GROUP = ? AND JOB_NAME = ? AND JOB_STEP = ?", maxUpdateDate, stats.getTotalRow(), step.getContext().getGroup(), step.getContext().getName(), step.getId());
            step.finished(this);
            LOGGER.info("[{}]组件执行完成,{}", getName(), stats.toString());
        } catch (InterruptedException ie) {
        } catch (Exception e) {
            e.printStackTrace();
            try {
                LOGGER.info("[{}]组件执行异常操作", getName());
                doOnError();
            } catch (Exception e1) {
            } finally {
                step.error(this);
            }
        } finally {
            try {
                finished();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public void send(SendEvent event) {
        if(step.hasNextStep()) {
            step.sendToNext(this, event);
        }
    }

    protected void beforeFinished() throws Exception{
    }

    @Override
    public void finished() throws Exception {
    }

    @Override
    public void doOnError() throws Exception {
    }

    public int getId() {
        return this.id;
    }

    public String getName() {
        return step.getName() + "-" + getId();
    }

    public StepMetaV2 getStepMeta() {
        return this.step.getStepMeta();
    }

    public StatisticsData getStats() {
        return stats;
    }

    public void setStep(IStep step) {
        this.step = step;
    }

    public void setId(int id) {
        this.id = id;
    }

    public void setEvents(ArrayBlockingQueue<SendEvent> events) {
        this.events = events;
    }

}
