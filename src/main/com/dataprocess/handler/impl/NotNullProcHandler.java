package com.ropeok.dataprocess.handler.impl;

import com.google.common.base.Preconditions;
import com.ropeok.dataprocess.meta.ColumnMeta;
import com.ropeok.dataprocess.handler.HandleStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;

public class NotNullProcHandler extends AbstractProcHandler{

    public static final String PROC_HANDLER_TYPE = "NotNull";
    private static final Logger LOGGER = LoggerFactory.getLogger(NotNullProcHandler.class);

    @Override
    public HandleStatus handle(Map<String, Object> data) throws Exception {
        Preconditions.checkNotNull(procHandlerMeta, "ProcHandlerMeta is not null");
        Iterator<ColumnMeta> it = procHandlerMeta.getColumnMetas().iterator();//rowMeta.getColumnMetas().iterator();
        while(it.hasNext()) {
            ColumnMeta cm = it.next();
            if(data.get(cm.getColumnName())== null) {
                data.put(HANDLER_ERROR_KEY, PROC_HANDLER_TYPE+":"+cm.getColumnName());
                return HandleStatus.FAIL;
            }
        }
        if(nextHandler != null) {
            return nextHandler.handle(data);
        }
        return HandleStatus.SUCC;
    }

}
