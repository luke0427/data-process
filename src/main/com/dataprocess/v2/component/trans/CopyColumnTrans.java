package com.ropeok.dataprocess.v2.component.trans;

import com.google.common.base.Preconditions;
import com.ropeok.dataprocess.utils.Constants;
import com.ropeok.dataprocess.v2.component.AbstractComponent;
import com.ropeok.dataprocess.v2.core.SendEvent;

import java.util.Collection;
import java.util.Map;

public class CopyColumnTrans extends AbstractComponent {

    private Collection<Map<String, Object>> datas;
    private String column;
    private String[] toColumns;
    private Object value;

    @Override
    public void init() throws Exception {
        column = getStepMeta().getStringProperty(Constants.COLUMN);
        toColumns = getStepMeta().getStringPropertyToArray(Constants.TO_COLUMNS);
        Preconditions.checkNotNull(column);
        Preconditions.checkNotNull(toColumns);
    }

    @Override
    public void exec(SendEvent event) throws Exception {
        if(event != null) {
            datas = event.getDataSet().getData();
            for(Map<String, Object> data : datas) {
                value = data.get(column);
                for(String toColumn : toColumns) {
                    data.put(toColumn.trim(), value);
                }
            }
        }
        send(event);
    }
}
