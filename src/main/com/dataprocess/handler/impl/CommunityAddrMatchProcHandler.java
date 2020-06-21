package com.ropeok.dataprocess.handler.impl;

import com.ropeok.dataprocess.handler.HandleStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * 定制化：社区管控地址匹配
 */
public class CommunityAddrMatchProcHandler extends JDBCCacheProcHandler{

    private static final Logger LOGGER = LoggerFactory.getLogger(CommunityAddrMatchProcHandler.class);
    private static final String ZAQX = "zaqx";
    private static final String LM = "lm";
    private static final String HZ = "hz";
    private String zaqxValue = null;
    private String lmValue = null;
    private String hzValue = null;
    private Map<String, Object> cacheRow = null;

    private int count = 0;

    @Override
    public HandleStatus handle(Map<String, Object> data) throws Exception {
        if(!"true".equals(procHandlerMeta.getMultiValue())) {
            zaqxValue = (String) data.get(ZAQX);
            lmValue = (String) data.get(LM);
            hzValue = (String) data.get(HZ);

            cacheRow = (Map<String, Object>) CACHE_MAP.get(zaqxValue+lmValue+hzValue);
            if(cacheRow == null) {
                cacheRow = (Map<String, Object>) CACHE_MAP.get(zaqxValue+lmValue);
            }
            if(cacheRow == null) {
                return HandleStatus.THROW;
            } else {
                data.put(procHandlerMeta.getColumnMetas().get(0).getToName(), cacheRow.get(procHandlerMeta.getColumnMetas().get(0).getColumnName()));
                /*if("福建省厦门市集美区乐安北里".equals(data.get(procHandlerMeta.getColumnMetas().get(0).getToName()))) {
                    LOGGER.info("命中总数:{}, 命中人员ID：{}, 地址名称:{}", ++count, data.get("ryid"), data.get("dzmc"));
                }*/
//                LOGGER.info("命中社区信息：{}", data);
            }
        }

        if(nextHandler != null) {
            return nextHandler.handle(data);
        }
        return HandleStatus.SUCC;
    }
}
