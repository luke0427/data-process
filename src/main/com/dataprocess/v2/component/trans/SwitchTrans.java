package com.ropeok.dataprocess.v2.component.trans;

import com.google.common.base.Preconditions;
import com.ropeok.dataprocess.utils.Constants;
import com.ropeok.dataprocess.v2.component.AbstractComponent;
import com.ropeok.dataprocess.v2.core.IDataSet;
import com.ropeok.dataprocess.v2.core.SendEvent;
import com.ropeok.dataprocess.v2.core.impl.DataSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SwitchTrans extends AbstractComponent {

    private static final Logger LOGGER = LoggerFactory.getLogger(SwitchTrans.class);
    private Collection<Map<String, Object>> datas;
    private String column;
    private String value;
    private Map<String, String> cases = new HashMap<>();
    private String stepKey;

    @Override
    public void init() throws Exception {
        this.column = getStepMeta().getStringProperty(Constants.COLUMN);
        String[] strCases = getStepMeta().getStringPropertyToArray(Constants.SWITCH_CASE);
        String[] kv;
        for(String cs : strCases) {
            kv = cs.split(":");
            Preconditions.checkArgument(kv.length == 2);
            cases.put(kv[0], kv[1]);
        }
        LOGGER.info("分支条件：{}", cases);
    }

    @Override
    public void exec(SendEvent event) throws Exception {
        if(event != null) {
            datas = event.getDataSet().getData();
            for(Map<String, Object> data : datas) {
                value = (String) data.get(column);
                if(value != null) {
                    stepKey = cases.get(value);
                    if(stepKey != null) {
                        IDataSet dataSet = new DataSet();
                        Map<String, Object> row = data;
                        dataSet.addData(data);
                        step.sendToNextStep(this, new SendEvent(dataSet), stepKey);
                    }
                    //不符合条件的值直接丢弃
                }
            }
        }
        event = null;
    }
}
