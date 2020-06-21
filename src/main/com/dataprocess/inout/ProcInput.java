package com.ropeok.dataprocess.inout;

import com.ropeok.dataprocess.meta.ProcInputOutputMeta;

import java.util.Map;

public interface ProcInput {
    public void setProcInputMeta(ProcInputOutputMeta procInputMeta);
    public ProcInputOutputMeta getProcInputMeta();
    public boolean hasNext() throws Exception;
    public Map<String, Object> next() throws Exception;
    public void init() throws Exception;
    public void finished() throws Exception;
    public void onError() throws Exception;
}
