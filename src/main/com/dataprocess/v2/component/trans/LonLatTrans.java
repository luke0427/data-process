package com.ropeok.dataprocess.v2.component.trans;

import com.google.common.base.Preconditions;
import com.ropeok.dataprocess.utils.Constants;
import com.ropeok.dataprocess.v2.component.AbstractComponent;
import com.ropeok.dataprocess.v2.core.SendEvent;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;

public class LonLatTrans extends AbstractComponent {

    private static final Logger LOGGER = LoggerFactory.getLogger(LonLatTrans.class);
    private Collection<Map<String, Object>> datas;
    private String lonColumn;
    private String latColumn;
    private String toColumn;
    private String value_0;
    private String value_1;
    private float lonValue = 0f;
    private float latValue = 0f;

    @Override
    public void init() throws Exception {
        String[] columns = getStepMeta().getStringPropertyToArray(Constants.COLUMNS);
        Preconditions.checkArgument(columns != null && columns.length == 2, "经纬度字段配置有误");
        lonColumn = columns[0].trim();
        latColumn = columns[1].trim();
        toColumn = getStepMeta().getStringProperty(Constants.TO_COLUMN);
    }

    @Override
    public void exec(SendEvent event) throws Exception {
        if(event != null) {
            datas = event.getDataSet().getData();
            for(Map<String, Object> data : datas) {
                value_0 = (String) data.get(lonColumn);
                value_1 = (String) data.get(latColumn);
                if(StringUtils.isNotBlank(value_0) && StringUtils.isNotBlank(value_1)) {
                    try {
                        lonValue = Float.valueOf(value_0);
                        latValue = Float.valueOf(value_1);
                        if(lonValue > 0 && lonValue <= 180 && latValue > 0 && latValue <= 90) {
                            data.put(toColumn, latValue + "," + lonValue);
                        }
                    } catch (NumberFormatException nfe) {
                        LOGGER.warn("Wrong LonLat Number{},{}", value_0, value_1);
                    }
                }
                data.remove(lonColumn);
                data.remove(latColumn);
            }
        }
        send(event);
    }
}
