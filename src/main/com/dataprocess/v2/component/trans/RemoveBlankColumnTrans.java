package com.ropeok.dataprocess.v2.component.trans;

import com.ropeok.dataprocess.utils.Constants;
import com.ropeok.dataprocess.v2.component.AbstractComponent;
import com.ropeok.dataprocess.v2.core.SendEvent;
import org.apache.commons.lang3.StringUtils;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

public class RemoveBlankColumnTrans extends AbstractComponent {

    private String[] columns;
    private Object value;
    private Collection<Map<String, Object>> datas;
    private Iterator<Map.Entry<String, Object>> it;
    private Map.Entry<String, Object> entry;

    @Override
    public void init() throws Exception {
        columns = getStepMeta().getStringPropertyToArray(Constants.COLUMNS);
    }

    @Override
    public void exec(SendEvent event) throws Exception {
        if(event != null) {
            datas = event.getDataSet().getData();
            if(columns != null) {
                for(Map<String, Object> data : datas) {
                    for(String column : columns) {
                        value = data.get(column);
                        if(value == null || StringUtils.isBlank(String.valueOf(value))) {
                            data.remove(column);
                        }
                    }
                }
            } else {
                for(Map<String, Object> data : datas) {
                    it = data.entrySet().iterator();
                    while (it.hasNext()) {
                        entry = it.next();
                        value = entry.getValue();
                        if(value == null || StringUtils.isBlank(String.valueOf(value))) {
                            data.remove(entry.getKey());
                        }
                    }
                }
            }
        }
        send(event);
    }
}
