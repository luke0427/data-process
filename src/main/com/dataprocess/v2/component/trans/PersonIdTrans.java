package com.ropeok.dataprocess.v2.component.trans;

import com.ropeok.dataprocess.utils.Constants;
import com.ropeok.dataprocess.v2.component.AbstractComponent;
import com.ropeok.dataprocess.v2.core.SendEvent;
import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;

public class PersonIdTrans extends AbstractComponent {

    private static final Logger LOGGER = LoggerFactory.getLogger(PersonIdTrans.class);
    private Collection<Map<String, Object>> datas;
    private String column;
    private String toColumn;
    private String prefix;
    private Object value;

    @Override
    public void init() throws Exception {
        column = getStepMeta().getStringProperty(Constants.COLUMN);
        toColumn = getStepMeta().getStringProperty(Constants.TO_COLUMN);
        prefix = getStepMeta().getStringProperty(Constants.PREFIX);
    }

    @Override
    public void exec(SendEvent event) throws Exception {
        if(event != null) {
            datas = event.getDataSet().getData();
            for(Map<String, Object> data : datas) {
                value = data.get(toColumn);
//                LOGGER.info("value={}, value==null:{}", value, value == null);
                if(value == null || String.valueOf(value).trim().length() == 0) {
                    value = data.get(column);
                    data.put(toColumn, prefix + DigestUtils.md5Hex(String.valueOf(value)).toUpperCase());
                }
            }
        }
        send(event);
    }
}
