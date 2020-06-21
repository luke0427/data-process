package com.ropeok.dataprocess.inout;


import com.ropeok.dataprocess.inout.impl.ESInputOutput;
import com.ropeok.dataprocess.inout.impl.RdbInputOutput;
import com.ropeok.dataprocess.inout.input.JDBCProcInput;

public enum InputOutputType {

    JDBC(RdbInputOutput.class),
    ES(ESInputOutput.class),
    OTHER(null);

    private Class<? extends InputOutput> clazz;
    InputOutputType(Class<? extends InputOutput> clazz) {

            this.clazz = clazz;
        }
    /*private Class<?> clazz;
    InputOutputType(Class<?> clazz) {
        this.clazz = clazz;
    }*/

    public static InputOutputType getInputOutputType(String type) {
        for(InputOutputType t : values()) {
            if(t.name().equals(type.toUpperCase())) {
                return t;
            }
        }
        return null;
    }
}
