package com.ropeok.dataprocess.handler.impl;

import com.ropeok.dataprocess.handler.HandleStatus;
import com.ropeok.dataprocess.meta.ColumnMeta;
import org.apache.commons.lang3.StringUtils;

import java.security.KeyStore;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class RemoveBlankProcHandler extends AbstractProcHandler{

    private Iterator<ColumnMeta> it = null;
    private List<ColumnMeta> columnMetas = null;
    private Iterator<Map.Entry<String, Object>> entryIterator = null;
    private Object value = null;
    private Map.Entry<String, Object> entry = null;

    @Override
    public HandleStatus handle(Map<String, Object> data) throws Exception {
        columnMetas = procHandlerMeta.getColumnMetas();
        if(columnMetas == null) {
            entryIterator = data.entrySet().iterator();
            while(entryIterator.hasNext()) {
                entry = entryIterator.next();
                value = entry.getValue();
                if(value == null || StringUtils.isBlank(String.valueOf(value))) {
                    entryIterator.remove();
                }
            }
            if(data.size() == 0) {
                return HandleStatus.THROW;
            }
        } else {
            it = procHandlerMeta.getColumnMetas().iterator();
            while(it.hasNext()) {
                ColumnMeta cm = it.next();
                if(data.get(cm.getColumnName()) == null || StringUtils.isBlank(String.valueOf(data.get(cm.getColumnName())))) {
                    data.remove(cm.getColumnName());
                }
            }
        }
        if(nextHandler != null) {
            nextHandler.handle(data);
        }
        return HandleStatus.SUCC;
    }
}
