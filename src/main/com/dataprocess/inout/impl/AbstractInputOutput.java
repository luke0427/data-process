package com.ropeok.dataprocess.inout.impl;

import com.ropeok.dataprocess.inout.InputOutput;
import com.ropeok.dataprocess.inout.InputOutputType;
import com.ropeok.dataprocess.meta.IncreMeta;

public class AbstractInputOutput implements InputOutput{
    protected int batchSize;
    protected InputOutputType type;
    protected String strType;
    protected IncreMeta increMeta;

    @Override
    public IncreMeta getIncreMeta() {
        return increMeta;
    }

    @Override
    public void setIncreMeta(IncreMeta increMeta) {
        this.increMeta = increMeta;
    }

    public String getStrType() {
        return strType == null ? type.name() : strType;
    }

    public void setStrType(String strType) {
        this.strType = strType;
    }

    @Override
    public int getBatchSize() {
        return batchSize;
    }

    @Override
    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    @Override
    public InputOutputType getType() {
        return type;
    }

    @Override
    public void setType(InputOutputType type) {
        this.type = type;
        this.strType = type.name();
    }
}
