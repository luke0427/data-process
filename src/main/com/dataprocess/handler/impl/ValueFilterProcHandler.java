package com.ropeok.dataprocess.handler.impl;

import com.ropeok.dataprocess.handler.HandleStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;

public class ValueFilterProcHandler extends AbstractProcHandler{

    private static final Logger LOGGER = LoggerFactory.getLogger(ValueFilterProcHandler.class);

    private Object value;
    private Iterator<Object> values;
    private boolean isThrow;
    @Override
    public HandleStatus handle(Map<String, Object> data) throws Exception {
        isThrow = true;
        value = data.get(procHandlerMeta.getColumn());
        values = procHandlerMeta.getValues().iterator();
        while (values.hasNext()) {
            if(value != null && value.toString().equals(values.next().toString())) {
                isThrow = false;
                break;
            }
        }
        if(isThrow) {
            return HandleStatus.THROW;
        }
        if(nextHandler != null) {
            nextHandler.handle(data);
        }
        return HandleStatus.SUCC;
    }
}
