package com.ropeok.dataprocess.handler.impl;

import com.ropeok.dataprocess.handler.HandleStatus;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class LonLatProcHandler extends AbstractProcHandler{

    private static final Logger LOGGER = LoggerFactory.getLogger(LonLatProcHandler.class);
    private String cond_1 = null;
    private String cond_2 = null;
    private float value_1 = 0f;
    private float value_2 = 0f;

    @Override
    public HandleStatus handle(Map<String, Object> data) throws Exception {
        cond_1 = (String) data.get(procHandlerMeta.getColumnMetas().get(0).getColumnName());
        cond_2 = (String) data.get(procHandlerMeta.getColumnMetas().get(1).getColumnName());
        if(StringUtils.isNotBlank(cond_1) && StringUtils.isNotBlank(cond_2)) {
            try {
                value_1 = Float.valueOf(cond_1);
                value_2 = Float.valueOf(cond_2);
                if(value_1 > 0 && value_1 <= 180 && value_2 > 0 && value_2 <= 90) {
                    data.put(procHandlerMeta.getTocolumn(), value_2 + "," + value_1);
                }
            } catch (NumberFormatException nfe) {
                LOGGER.warn("Wrong LonLat Number{},{}", value_1, value_2);
            }
        }
        data.remove(procHandlerMeta.getColumnMetas().get(0).getColumnName());
        data.remove(procHandlerMeta.getColumnMetas().get(1).getColumnName());
        if(nextHandler != null) {
            return nextHandler.handle(data);
        }
        return HandleStatus.SUCC;
    }
}
