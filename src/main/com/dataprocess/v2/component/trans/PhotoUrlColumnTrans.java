package com.ropeok.dataprocess.v2.component.trans;

import com.google.common.base.Preconditions;
import com.ropeok.dataprocess.utils.Constants;
import com.ropeok.dataprocess.v2.component.AbstractComponent;
import com.ropeok.dataprocess.v2.core.SendEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;

public class PhotoUrlColumnTrans extends AbstractComponent {

    private static final Logger LOGGER = LoggerFactory.getLogger(PhotoUrlColumnTrans.class);
    private String toColumn;
    private String dirName;
    private String paramColumn;
    private String nameColumn;
    private String dataColumn;
    private byte[] picData;
    private Collection<Map<String, Object>> datas;

    @Override
    public void init() throws Exception {
        toColumn = getStepMeta().getStringProperty(Constants.TO_COLUMN);
        dirName = getStepMeta().getStringProperty(Constants.DIR_NAME);
        paramColumn = getStepMeta().getStringProperty(Constants.PARAM_COLUMN);
        nameColumn = getStepMeta().getStringProperty(Constants.NAME_COLUMN);
        dataColumn = getStepMeta().getStringProperty(Constants.DATA_COLUMN);
        Preconditions.checkNotNull(dataColumn);
    }

    @Override
    public void exec(SendEvent event) throws Exception {
        if(event != null) {
            datas = event.getDataSet().getData();
            for(Map<String, Object> data : datas) {
                picData = (byte[]) data.get(dataColumn);
                if(picData != null && picData.length > 0) {
                    data.put(toColumn, "/" + dirName + "/" + String.valueOf(data.get(paramColumn)) + "/" + String.valueOf(data.get(nameColumn)) + ".jpg");
                }
            }
        }
        send(event);
    }

}
