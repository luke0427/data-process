package com.ropeok.dataprocess.handler.impl;

import com.google.common.base.Preconditions;
import com.ropeok.dataprocess.handler.HandleStatus;
import com.ropeok.dataprocess.meta.ColumnMeta;
import org.apache.commons.lang3.StringUtils;

import java.util.Iterator;
import java.util.Map;

public class TrimProcHandler extends AbstractProcHandler{

    @Override
    public HandleStatus handle(Map<String, Object> data) throws Exception {
        Preconditions.checkNotNull(procHandlerMeta, "ProcHandlerMeta is not null");
        Iterator<ColumnMeta> it = procHandlerMeta.getColumnMetas().iterator();
        while(it.hasNext()) {
            ColumnMeta cm = it.next();
            if(data.get(cm.getColumnName()) != null) {
                data.put(cm.getColumnName(), StringUtils.trim((String)data.get(cm.getColumnName())));
            }
        }
        if(nextHandler != null) {
            nextHandler.handle(data);
        }
        return HandleStatus.SUCC;
    }

}
