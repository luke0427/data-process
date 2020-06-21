package com.ropeok.dataprocess.handler.impl;

import com.ropeok.dataprocess.handler.HandleStatus;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.text.ParseException;
import java.util.Date;
import java.util.Map;

public class DateFormatProcHandler extends AbstractProcHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(DateFormatProcHandler.class);

    private String format;
    private Object value;

    @Override
    public void init() {
        super.init();
        this.format = procHandlerMeta.getFormat();
    }

    @Override
    public HandleStatus handle(Map<String, Object> data) throws Exception {
        try {
            value = data.get(procHandlerMeta.getColumn());
            if(value instanceof Date) {
                data.put(procHandlerMeta.getColumn(), DateFormatUtils.format((Date) value, format));
            } else if(value != null){
                data.put(procHandlerMeta.getColumn(), DateFormatUtils.format(DateUtils.parseDate((String) value, format), format));
            }
        } catch (ParseException pe) {
            LOGGER.warn("column={} parse error value={}.", procHandlerMeta.getColumn(), value);
            data.remove(procHandlerMeta.getColumn());
        }

        if(nextHandler != null) {
            return nextHandler.handle(data);
        }
        return HandleStatus.SUCC;
    }

}
