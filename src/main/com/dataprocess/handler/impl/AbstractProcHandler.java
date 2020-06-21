package com.ropeok.dataprocess.handler.impl;

import com.ropeok.dataprocess.handler.ProcHandler;
import com.ropeok.dataprocess.meta.ProcHandlerMeta;

public abstract class AbstractProcHandler implements ProcHandler {

    public static final String HANDLER_ERROR_KEY = "ERROR_HANDLER";

    protected ProcHandler nextHandler;
    protected ProcHandlerMeta procHandlerMeta;

    public void init() {
    }

    @Override
    public ProcHandlerMeta getProcHandlerMeta() {
        return procHandlerMeta;
    }

    @Override
    public void setProcHandlerMeta(ProcHandlerMeta procHandlerMeta) {
        this.procHandlerMeta = procHandlerMeta;
    }

    public ProcHandler getNextHandler() {
        return nextHandler;
    }

    public void setNextHandler(ProcHandler handler) {
        this.nextHandler = handler;
    }

}
