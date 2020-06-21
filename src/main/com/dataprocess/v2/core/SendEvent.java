package com.ropeok.dataprocess.v2.core;

import java.io.Serializable;
import java.util.Map;

/**
 * 发送事件，持有数据对象，事件状态
 */
public class SendEvent implements Serializable{

    public enum EventStatus {
        DATA, FINISHED
    }

    private EventStatus eventStatus = EventStatus.DATA;

    private IDataSet dataSet;

    public SendEvent(EventStatus eventStatus) {
        this.eventStatus = eventStatus;
    }

    public SendEvent(IDataSet dataSet) {
        this.dataSet = dataSet;
    }

    public void setDataSet(IDataSet dataSet) {
        this.dataSet = dataSet;
    }

    public IDataSet<Map<String, Object>> getDataSet() {
        return dataSet;
    }

    public EventStatus getEventStatus() {
        return eventStatus;
    }

    public void setEventStatus(EventStatus eventStatus) {
        this.eventStatus = eventStatus;
    }

    @Override
    public String toString() {
        return eventStatus + ", " + dataSet;
    }

    /*public Object clone() {
        SendEvent sendEvent = null;
        try {
            sendEvent = (SendEvent) super.clone();
            sendEvent.setDataSet((IDataSet) this.getDataSet().clone());
        } catch (CloneNotSupportedException e) {
            e.printStackTrace();
        }
        return sendEvent;
    }*/
}
