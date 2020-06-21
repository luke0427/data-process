package com.ropeok.dataprocess.v2.component.trans;

import com.ropeok.dataprocess.utils.Constants;
import com.ropeok.dataprocess.v2.component.AbstractComponent;
import com.ropeok.dataprocess.v2.core.SendEvent;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

public class SaveColumnTrans extends AbstractComponent {

    private Collection<Map<String, Object>> datas;
    private String[] columns;
    private Iterator<String> keys;

    @Override
    public void init() throws Exception {
        columns = getStepMeta().getStringPropertyToArray(Constants.COLUMNS);
    }

    @Override
    public void exec(SendEvent event) throws Exception {
        /*if(event != null) {
            datas = event.getDataSet().getData();
            for(Map<String, Object> data : datas) {
                暂时不提供这个功能
            }
        }*/
        send(event);
    }
}
