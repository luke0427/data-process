package com.ropeok.dataprocess.handler.impl;

import com.ropeok.dataprocess.handler.HandleStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * 方便调试
 */
public class PrintDataProcHandler extends AbstractProcHandler{

    private static final Logger LOGGER = LoggerFactory.getLogger(PrintDataProcHandler.class);

    private int count = 0;
    @Override
    public HandleStatus handle(Map<String, Object> data) throws Exception {
        /*if(((String)data.get("dzmc")).contains("福建省厦门市集美区乐安北里")) {
            LOGGER.info("count:{}, data:{}", ++count, data);
        }*/
        LOGGER.info("count:{}, data:{}", ++count, data);
        return HandleStatus.THROW;
    }
}
