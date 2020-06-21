package com.ropeok.dataprocess.handler.impl;

import com.google.common.base.Preconditions;
import com.ropeok.dataprocess.meta.ColumnMeta;
import com.ropeok.dataprocess.handler.HandleStatus;
import org.apache.commons.codec.digest.DigestUtils;

import java.util.Iterator;
import java.util.Map;

public class PKMD5ProcHandler extends AbstractProcHandler{

    public static final String PROC_HANDLER_TYPE = "PKMD5";
    private StringBuilder value = new StringBuilder();
    private String toColumn = null;

    public PKMD5ProcHandler() {
        /*super.type = PROC_HANDLER_TYPE;*/
    }

    @Override
    public HandleStatus handle(Map<String, Object> data) throws Exception {
        Preconditions.checkNotNull(procHandlerMeta, "ProcHandlerMeta is not null");
        Iterator<ColumnMeta> it = procHandlerMeta.getColumnMetas().iterator();
        toColumn = toColumn == null ? procHandlerMeta.getTocolumn(): toColumn;
        value.setLength(0);

        while(it.hasNext()) {
            ColumnMeta cm = it.next();
            value.append(data.get(cm.getColumnName()));
        }
        data.put(toColumn, DigestUtils.md5Hex(value.toString()));

        if(nextHandler != null) {
            return nextHandler.handle(data);
        }
        return HandleStatus.SUCC;
    }
}
