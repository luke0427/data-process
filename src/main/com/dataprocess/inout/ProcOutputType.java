package com.ropeok.dataprocess.inout;

import com.ropeok.dataprocess.inout.output.ESProcOutput;
import com.ropeok.dataprocess.inout.output.RedisProcOutput;

public enum ProcOutputType {

    ESProcOutput(ESProcOutput.class),
    RedisProcOutput(RedisProcOutput.class);

    private Class<? extends ProcOutput> clazz;

    ProcOutputType(Class<? extends ProcOutput> clazz) {
        this.clazz = clazz;
    }

    public Class<? extends ProcOutput> getClazz() {
        return this.clazz;
    }

    public static ProcOutputType getProcOutputType(String type) {
        for(ProcOutputType t : values()) {
            if(t.name().equals(type)) {
                return t;
            }
        }
        return null;
    }
}
