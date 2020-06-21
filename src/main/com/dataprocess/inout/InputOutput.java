package com.ropeok.dataprocess.inout;

import com.ropeok.dataprocess.meta.IncreMeta;

public interface InputOutput {
    public void setType(InputOutputType type);
    public InputOutputType getType();
    public void setBatchSize(int batchSize);
    public int getBatchSize();
    public void setIncreMeta(IncreMeta increMeta);
    public IncreMeta getIncreMeta();
}
