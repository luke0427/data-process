package com.ropeok.dataprocess.v2.component.trans;

import com.ropeok.dataprocess.utils.CommUtils;
import com.ropeok.dataprocess.utils.Constants;
import com.ropeok.dataprocess.utils.DateUtils;
import com.ropeok.dataprocess.v2.component.AbstractComponent;
import com.ropeok.dataprocess.v2.core.SendEvent;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SfzhValidTrans extends AbstractComponent {

    private static final Logger LOGGER = LoggerFactory.getLogger(SfzhValidTrans.class);
    private static final String TYPE_OTHER = "0";//证件类型：其他
    private static final String TYPE_SFZ = "1";//证件类型：身份证
    private static final Pattern pattern = Pattern.compile("(^\\d{15}$)|(^\\d{18}$)|(^\\d{17}(\\d|X|x)$)");
    private Matcher matcher = null;
    private String column;
    private String typeColumn;
    private Collection<Map<String, Object>> datas;
    private String originSfzh;
    private String stdSfzh;
    private boolean isThrow;
    private boolean isFormat;
    private String birthdayColumn;
    private String sexColumn;
    private int year;
    private int month;
    private int day;

    @Override
    public void init() throws Exception {
        column = getStepMeta().getStringProperty(Constants.COLUMN);
        isThrow = getStepMeta().getBooleanProperty(Constants.THROW);
        typeColumn = getStepMeta().getStringProperty(Constants.TYPE_COLUMN);
        isFormat = getStepMeta().getOrDefaultBooleanProperty(Constants.FORMAT, true);
        birthdayColumn = getStepMeta().getStringProperty(Constants.BIRTHDAY_COLUMN);
        sexColumn = getStepMeta().getStringProperty(Constants.SEX_COLUMN);
    }

    @Override
    public void exec(SendEvent event) throws Exception {
        //验证身份证是否合法。不合法：可以忽略或者过滤
        if(event != null) {
            datas = event.getDataSet().getData();
            for(Map<String, Object> data : datas) {
                originSfzh = data.get(column) != null ? data.get(column).toString().trim() : null;
                if(StringUtils.isNotEmpty(originSfzh)) {
                    matcher = pattern.matcher(originSfzh);
                    if(!matcher.matches()) {
                        if(!unMatched(data)) {
                            datas.remove(data);
                        }
                    } else {
                        if(!matched(data)) {
                            datas.remove(data);
                        }
                    }
                }
            }
        }
        send(event);
    }

    private boolean matched(Map<String, Object> data) {
        if(typeColumn != null) {
            data.put(typeColumn, TYPE_SFZ);
        }
        if(isFormat) {
            stdSfzh = originSfzh.length() == 15 ? CommUtils.transIDCard15to18(originSfzh) : originSfzh;
            if(StringUtils.isNotBlank(stdSfzh)) {
                data.put(column, stdSfzh);
                if(birthdayColumn != null) {
                    year = Integer.parseInt(stdSfzh.substring(6,10));
                    month = Integer.parseInt(stdSfzh.substring(10, 12));
                    day = Integer.parseInt(stdSfzh.substring(12, 14));
                    if(DateUtils.isValidDate(year, month, day)) {
                        data.put(birthdayColumn, year + "-" + stdSfzh.substring(10, 12) + "-" + stdSfzh.substring(12, 14));
                    }
                }
                if(sexColumn != null) {
                    data.put(sexColumn, Integer.parseInt(stdSfzh.substring(16,17)) % 2 == 0 ? "女": "男");
                }
            } else {
                LOGGER.error("身份证转换失败:原始身份证号={}", originSfzh);
                if(isThrow) {
                    return false;
                }
                if(typeColumn != null) {
                    data.put(typeColumn, TYPE_OTHER);
                }
            }
        }
        return true;
    }

    private boolean unMatched(Map<String, Object> data) {
        if(isThrow) {
            return false;
        }
        if(typeColumn != null) {
            data.put(typeColumn, TYPE_OTHER);
        }
        return true;
    }
}
