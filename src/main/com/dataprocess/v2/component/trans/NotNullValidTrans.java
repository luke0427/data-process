package com.ropeok.dataprocess.v2.component.trans;

import com.ropeok.dataprocess.utils.Constants;
import com.ropeok.dataprocess.v2.component.AbstractComponent;
import com.ropeok.dataprocess.v2.core.SendEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;

public class NotNullValidTrans extends AbstractComponent {

    private static final Logger LOGGER = LoggerFactory.getLogger(NotNullValidTrans.class);
    private String[] columns;
    private Collection<Map<String, Object>> datas;
    private boolean isPrint;

    @Override
    public void init() throws Exception {
        columns = getStepMeta().getStringPropertyToArray(Constants.COLUMNS);
        isPrint = getStepMeta().getBooleanProperty(Constants.PRINT);
    }

    @Override
    public void exec(SendEvent event) throws Exception {
        if(event != null) {
            datas = event.getDataSet().getData();
            for(Map<String, Object> data : datas) {
                for(String column : columns) {
                    if(data.get(column) == null) {
                        datas.remove(data);
                        if(isPrint) {
                            LOGGER.info("NotNullValid throw:{}", data);
                        }
                        break;
                    }
                }
            }
        }
        if(event != null && event.getDataSet().getData().size() > 0) {
            send(event);
        }
    }
}
