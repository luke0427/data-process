package com.ropeok.dataprocess.inout;

import com.ropeok.dataprocess.inout.input.ESProcInput;
import com.ropeok.dataprocess.inout.input.JDBCProcInput;
import com.ropeok.dataprocess.inout.input.ParamProcInput;

public enum ProcInputType {

    JDBCProcInput(JDBCProcInput.class),
    ESProcInput(ESProcInput.class),
    ParamProcInput(ParamProcInput.class);

    private Class<? extends ProcInput> clazz;

    ProcInputType(Class<? extends ProcInput> clazz) {
        this.clazz = clazz;
    }

    public Class<? extends ProcInput> getClazz() {
        return this.clazz;
    }

    public static ProcInputType getProcInputType(String type) {
        for(ProcInputType t : values()) {
            if(t.name().equals(type)) {
                return t;
            }
        }
        return null;
    }

}
