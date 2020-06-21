package com.ropeok.dataprocess.handler;

import com.ropeok.dataprocess.meta.ProcHandlerMeta;
import com.ropeok.dataprocess.meta.RowMeta;

import java.util.Map;

public interface ProcHandler {
    public HandleStatus handle(Map<String, Object> data) throws Exception;
    public void setNextHandler(ProcHandler procHandler);
//    public String getType();
//    public void setRowMeta(RowMeta rowMeta);
    public void setProcHandlerMeta(ProcHandlerMeta procHandlerMeta);
    public ProcHandlerMeta getProcHandlerMeta();
    public void init();
}
