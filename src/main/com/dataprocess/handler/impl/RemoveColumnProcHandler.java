package com.ropeok.dataprocess.handler.impl;

import com.ropeok.dataprocess.handler.HandleStatus;
import com.ropeok.dataprocess.meta.ColumnMeta;

import java.util.Map;

public class RemoveColumnProcHandler extends AbstractProcHandler{

    @Override
    public HandleStatus handle(Map<String, Object> data) throws Exception {
        for(ColumnMeta columnMeta : procHandlerMeta.getColumnMetas()) {
            data.remove(columnMeta.getColumnName());
        }
        if(nextHandler != null) {
            nextHandler.handle(data);
        }
        return HandleStatus.SUCC;
    }

}
