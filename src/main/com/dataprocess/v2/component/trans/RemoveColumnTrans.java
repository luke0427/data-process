package com.ropeok.dataprocess.v2.component.trans;

import com.ropeok.dataprocess.utils.Constants;
import com.ropeok.dataprocess.v2.component.AbstractComponent;
import com.ropeok.dataprocess.v2.core.SendEvent;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;

public class RemoveColumnTrans extends AbstractComponent {

    private static final Logger LOGGER = LoggerFactory.getLogger(RemoveColumnTrans.class);
    private String[] removeColumns;
    private boolean isEmpty;
    private Object value;
    private Collection<Map<String, Object>> datas;

    @Override
    public void init() throws Exception {
        removeColumns = getStepMeta().getStringPropertyToArray(Constants.COLUMNS);
        isEmpty = "empty".equals(getStepMeta().getStringProperty(Constants.MODE));
    }

    @Override
    public void exec(SendEvent event) throws Exception {
        if(event != null) {
            datas = event.getDataSet().getData();
            for(Map<String, Object> data : datas) {
                for(String column : removeColumns) {
                    if(isEmpty) {
                        value = data.get(column.trim());
                        if(value == null ||  StringUtils.isBlank(String.valueOf(value))) {
                            data.remove(column.trim());
                        }
                    } else {
                        data.remove(column.trim());
                    }
                }
            }
        }
        send(event);
    }
}
