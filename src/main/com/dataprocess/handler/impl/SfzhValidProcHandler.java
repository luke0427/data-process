package com.ropeok.dataprocess.handler.impl;

import com.google.common.base.Preconditions;
import com.ropeok.dataprocess.meta.ColumnMeta;
import com.ropeok.dataprocess.handler.HandleStatus;
import com.ropeok.dataprocess.utils.CommUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SfzhValidProcHandler extends AbstractProcHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(SfzhValidProcHandler.class);

    public static final String PROC_HANDLER_TYPE = "SfzhValid";

    private static final Pattern pattern = Pattern.compile("(^\\d{15}$)|(^\\d{18}$)|(^\\d{17}(\\d|X|x)$)");

    private Matcher matcher = null;
    private String sfzh = null;
    private int year;
    private int month;
    private int day;
    private Iterator<ColumnMeta> it;
    private ColumnMeta cm;

    @Override
    public HandleStatus handle(Map<String, Object> data) throws Exception {
        Preconditions.checkNotNull(procHandlerMeta, "ProcHandlerMeta is not null");
        it = procHandlerMeta.getColumnMetas().iterator();
        while(it.hasNext()) {
            cm = it.next();
            sfzh = data.get(cm.getColumnName()) != null ? data.get(cm.getColumnName()).toString().trim() : "";
            matcher = pattern.matcher(sfzh);
            if(!matcher.matches()) {
                if("true".equals(procHandlerMeta.getIgnore())) {
                    if("true".equals(procHandlerMeta.getPickType())) {
//                        data.put("zjlx", "0");
                        data.put(procHandlerMeta.getTypeColumn(), "0");
                    }
//                    return HandleStatus.SUCC;
                } else {
                    data.put(HANDLER_ERROR_KEY, PROC_HANDLER_TYPE +":"+sfzh);
                    return HandleStatus.FAIL;
                }
            } else {
                if(sfzh != null && sfzh.length() == 15 && !"false".equals(procHandlerMeta.getFormat())) {
                    sfzh = CommUtils.transIDCard15to18(sfzh);
                    if(StringUtils.isNotBlank(sfzh)){
                        data.put(cm.getColumnName(), sfzh);
                    } else {
                        LOGGER.error("身份证转换失败:原始身份证号={}", data.get(cm.getColumnName()));
                        data.put(HANDLER_ERROR_KEY, PROC_HANDLER_TYPE +":"+data.get(cm.getColumnName()));
                        return HandleStatus.FAIL;
                    }
                }
                if("true".equals(procHandlerMeta.getPickType())) {
//                    data.put("zjlx", "1");//TODO: 暂时写死
                    data.put(procHandlerMeta.getTypeColumn(), "1");
                }
                if("true".equals(procHandlerMeta.getPickBirth())) {
                    //TODO: 从身份证中提取出生日期，暂时写死
//                    data.put("csrq", sfzh.substring(6,10) + "-" + sfzh.substring(10, 12) + "-" + sfzh.substring(12, 14));
                    year = Integer.parseInt(sfzh.substring(6,10));
                    month = Integer.parseInt(sfzh.substring(10, 12));
                    day = Integer.parseInt(sfzh.substring(12, 14));
                    if(year >= 1900 && month > 0 && month <= 12 && day > 0 && day <= 31) {
                        data.put(procHandlerMeta.getBirthColumn(), sfzh.substring(6,10) + "-" + sfzh.substring(10, 12) + "-" + sfzh.substring(12, 14));
                    } else {
//                        LOGGER.warn("身份证号异常：{}", sfzh);//取消打印异常身份证号
                    }
                }
                if("true".equals(procHandlerMeta.getPickSex())) {
                    data.put(procHandlerMeta.getSexColumn(), Integer.parseInt(sfzh.substring(16,17)) % 2 == 0 ? "女": "男");
                }
            }
        }
        if(nextHandler != null) {
            return nextHandler.handle(data);
        }
        return HandleStatus.SUCC;
    }
}
