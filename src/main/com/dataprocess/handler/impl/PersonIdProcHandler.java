package com.ropeok.dataprocess.handler.impl;

import com.ropeok.dataprocess.handler.HandleStatus;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.Map;

public class PersonIdProcHandler extends AbstractProcHandler {

    private String value = null;

    private String PREFIX;

    private String toValue;

    @Override
    public void init() {
        PREFIX = procHandlerMeta.getKeyPrefix();
    }

    @Override
    public HandleStatus handle(Map<String, Object> data) throws Exception {
        value = (String) data.get(procHandlerMeta.getColumn());
        toValue = data.get(procHandlerMeta.getTocolumn()) == null ? null : (String) data.get(procHandlerMeta.getTocolumn());
        if(value != null && StringUtils.isBlank(toValue)) {
            data.put(procHandlerMeta.getTocolumn(), PREFIX + DigestUtils.md5Hex(value).toUpperCase());
        }
        if(nextHandler != null) {
            return nextHandler.handle(data);
        }
        return HandleStatus.SUCC;
    }
}
