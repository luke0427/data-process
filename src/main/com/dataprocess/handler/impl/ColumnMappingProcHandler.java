package com.ropeok.dataprocess.handler.impl;

import com.google.common.base.Preconditions;
import com.ropeok.dataprocess.meta.ColumnMeta;
import com.ropeok.dataprocess.handler.HandleStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;

public class ColumnMappingProcHandler extends AbstractProcHandler{

    private static final Logger LOGGER = LoggerFactory.getLogger(ColumnMappingProcHandler.class);
    public static final String PROC_HANDLER_TYPE = "ColumnMapping";

    private boolean retain = false;

    @Override
    public void init() {
        if(procHandlerMeta.existKey("retain")) {
            retain = Boolean.valueOf((String) procHandlerMeta.getParam("retain"));
        }
    }

    @Override
    public HandleStatus handle(Map<String, Object> data) throws Exception {
        Preconditions.checkNotNull(procHandlerMeta, "ProcHandlerMeta is not null");
        Iterator<ColumnMeta> it = procHandlerMeta.getColumnMetas().iterator();
        while(it.hasNext()) {
            ColumnMeta cm = it.next();
            data.put(cm.getToName(), data.get(cm.getColumnName()));
            if(!retain) {
                data.remove(cm.getColumnName());
            }
        }
        if(nextHandler != null) {
            return nextHandler.handle(data);
        }
        return HandleStatus.SUCC;
    }
}
