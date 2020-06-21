package com.ropeok.dataprocess.v2.component.trans;

import com.ropeok.dataprocess.utils.Constants;
import com.ropeok.dataprocess.v2.component.AbstractComponent;
import com.ropeok.dataprocess.v2.core.SendEvent;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.util.Collection;
import java.util.Date;
import java.util.Map;

public class DateFormatTrans extends AbstractComponent {

    private static final Logger LOGGER = LoggerFactory.getLogger(DateFormatTrans.class);
    private Collection<Map<String, Object>> datas;
    private String column;
    private String format;
    private String dateFormat;
    private Object value;

    @Override
    public void init() throws Exception {
        column = getStepMeta().getStringProperty(Constants.COLUMN);
        format = getStepMeta().getStringProperty(Constants.FORMAT);
        dateFormat = getStepMeta().getStringProperty(Constants.DATE_FORMAT);
    }

    @Override
    public void exec(SendEvent event) throws Exception {
        if(event != null) {
            datas = event.getDataSet().getData();
            for(Map<String, Object> data : datas) {
                value = data.get(column);
                try {
                    if(value instanceof Date) {
                        data.put(column, DateFormatUtils.format((Date) value, dateFormat));
                    }else if(value instanceof String){
                        data.put(column, DateFormatUtils.format(DateUtils.parseDate((String) value, format), dateFormat));
                    } else {
                        LOGGER.warn("unsupported dateformat type={}, value={}", value.getClass(), value);
                    }
                } catch (ParseException pe) {
                    LOGGER.warn("column={} parse error value={}.", column, value);
                    data.remove(column);
                }
            }
        }
        send(event);
    }
}
