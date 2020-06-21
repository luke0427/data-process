package com.ropeok.dataprocess.v2.component.trans;

import com.google.common.base.Preconditions;
import com.ropeok.dataprocess.utils.Constants;
import com.ropeok.dataprocess.v2.component.AbstractComponent;
import com.ropeok.dataprocess.v2.core.SendEvent;
import org.apache.commons.lang3.time.FastDateFormat;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.Collection;
import java.util.Map;

public class TypeMappingTrans extends AbstractComponent {

    private String[] columns;
    private String[] types;
    private int columnSize;
    private String dateFormat;
    private Collection<Map<String, Object>> datas;

    @Override
    public void init() throws Exception {
        this.columns = getStepMeta().getStringPropertyToArray(Constants.COLUMNS);
        this.types = getStepMeta().getStringPropertyToArray(Constants.TYPES);
        Preconditions.checkNotNull(columns, "未定义输入字段名称");
        Preconditions.checkNotNull(types, "未定义映射字段类型");
        this.columnSize = columns.length;
        Preconditions.checkArgument(types.length == columnSize, "字段与映射类型长度不一致");
        this.dateFormat = getStepMeta().getStringProperty(Constants.DATE_FORMAT);
    }

    @Override
    public void exec(SendEvent event) throws Exception {
        if(event != null) {
            datas = event.getDataSet().getData();
            for (Map<String, Object> data : datas) {
                for(int i = 0; i < columnSize; i++) {
                    if("DATE".equals(types[i])) {
                        data.put(columns[i], JavaToJDBC.toDate(data.get(columns[i]), dateFormat));
                    } else if("TIMESTAMP".equals(types[i])) {
                        data.put(columns[i], JavaToJDBC.toTimestamp(data.get(columns[i]), dateFormat));
                    }
                }
            }
        }
        send(event);
    }

    public static class JavaToJDBC {
        public static Date toDate(Object value, String pattern) throws Exception {
            if(value instanceof java.util.Date) {
                return new Date(((java.util.Date) value).getTime());
            } else {
                return new Date(FastDateFormat.getInstance(pattern).parse((String) value).getTime());
            }
        }

        public static Timestamp toTimestamp(Object value, String pattern) throws Exception {
            if(value instanceof java.util.Date) {
                return new Timestamp(((java.util.Date) value).getTime());
            } else {
                return new Timestamp(FastDateFormat.getInstance(pattern).parse((String) value).getTime());
            }
        }
    }
}
