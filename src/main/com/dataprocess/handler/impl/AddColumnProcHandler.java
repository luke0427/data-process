package com.ropeok.dataprocess.handler.impl;

import com.ropeok.dataprocess.handler.HandleStatus;
import com.ropeok.dataprocess.meta.ColumnMeta;
import org.apache.commons.lang3.StringUtils;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

public class AddColumnProcHandler extends AbstractProcHandler{

    private String dateValue;

    @Override
    public void init() {
        String format = procHandlerMeta.getFormat();
        if(StringUtils.isNotBlank(format)) {
            SimpleDateFormat dateFormat = new SimpleDateFormat(format);
            dateValue = dateFormat.format(new Date());
        }
    }

    @Override
    public HandleStatus handle(Map<String, Object> data) throws Exception {
        if(dateValue != null) {
            for(ColumnMeta cm : procHandlerMeta.getColumnMetas()) {
                data.put(cm.getColumnName(), dateValue);
            }
        } else {
            int i = 0;
            for (ColumnMeta cm : procHandlerMeta.getColumnMetas()) {
                data.put(cm.getColumnName(), procHandlerMeta.getValues().get(i++));
            }
        }
        if(nextHandler != null) {
            return nextHandler.handle(data);
        }
        return HandleStatus.SUCC;
    }
}
