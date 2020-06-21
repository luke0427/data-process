package com.ropeok.dataprocess.inout;

import com.ropeok.dataprocess.meta.ProcInputOutputMeta;

import java.util.Map;

public interface ProcOutput extends Runnable {
    public void setProcOutputMeta(ProcInputOutputMeta procOutputMeta);
    public ProcInputOutputMeta getProcOutputMeta();
    public void init();
    public void put(Map<String, Object> data);
    public void finished() throws Exception;
}
