package com.ropeok.dataprocess.v2.component.trans;

import com.ropeok.dataprocess.utils.Constants;
import com.ropeok.dataprocess.v2.component.AbstractComponent;
import com.ropeok.dataprocess.v2.core.SendEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;

public class PrintDataTrans extends AbstractComponent {

    private static final Logger LOGGER = LoggerFactory.getLogger(PrintDataTrans.class);

    private boolean isThrow;
    private int batchSize;

    @Override
    public void init() throws Exception {
        isThrow = getStepMeta().getBooleanProperty(Constants.THROW);
        batchSize = getStepMeta().getOrDefaultIntProperty(Constants.BATCH_SIZE, -1);
    }

    private Map<String, Object> data;
    @Override
    public void exec(SendEvent event) throws Exception {
        if(event != null) {
            Iterator<Map<String, Object>> it = event.getDataSet().getData().iterator();
            while(it.hasNext()) {
                data = it.next();
                if(batchSize == -1) {
                    LOGGER.info("total={}, data={}", stats.getTotalRow(), data);
                } else if(stats.getCurrentRow() >= batchSize) {
                    LOGGER.info("stats={}, data={}", stats.getCurrentInfo(), data);
                    stats.reset();
                }
            }
        }
        if(!isThrow) {
            send(event);
        }
    }

}
