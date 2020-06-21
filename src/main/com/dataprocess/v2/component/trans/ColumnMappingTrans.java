package com.ropeok.dataprocess.v2.component.trans;

import com.google.common.base.Preconditions;
import com.ropeok.dataprocess.utils.Constants;
import com.ropeok.dataprocess.v2.component.AbstractComponent;
import com.ropeok.dataprocess.v2.core.SendEvent;

import java.util.Collection;
import java.util.Map;

public class ColumnMappingTrans extends AbstractComponent {

    private Collection<Map<String, Object>> datas;
    private String[] columns;
    private String[] toColumns;
    private int size;

    @Override
    public void init() throws Exception {
        columns = getStepMeta().getStringPropertyToArray(Constants.COLUMNS);
        toColumns = getStepMeta().getStringPropertyToArray(Constants.TO_COLUMNS);
        size = columns.length;
        Preconditions.checkArgument(size == toColumns.length, "字段数目不匹配");
    }

    @Override
    public void exec(SendEvent event) throws Exception {
        if(event != null) {
            datas = event.getDataSet().getData();
            for(Map<String, Object> data : datas) {
                for(int i = 0; i < size; i++) {
                    data.put(toColumns[i], data.get(columns[i]));
                    data.remove(columns[i]);
                }
            }
        }
        send(event);
    }
}
