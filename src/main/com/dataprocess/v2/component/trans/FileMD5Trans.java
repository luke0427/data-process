package com.ropeok.dataprocess.v2.component.trans;

import com.ropeok.dataprocess.utils.Constants;
import com.ropeok.dataprocess.v2.component.AbstractComponent;
import com.ropeok.dataprocess.v2.core.SendEvent;
import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;

public class FileMD5Trans extends AbstractComponent {

    private static final Logger LOGGER = LoggerFactory.getLogger(FileMD5Trans.class);
    private String column;
    private String toColumn;
    private boolean isRemoveFile;
    private Collection<Map<String, Object>> datas;
    private Object value;

    @Override
    public void init() throws Exception {
        column = getStepMeta().getStringProperty(Constants.COLUMN);
        toColumn = getStepMeta().getStringProperty(Constants.TO_COLUMN);
        isRemoveFile = getStepMeta().getBooleanProperty(Constants.REMOVE_FILE);
    }

    @Override
    public void exec(SendEvent event) throws Exception {
        if(event != null) {
            datas = event.getDataSet().getData();
            for(Map<String, Object> data : datas) {
                value = data.get(column);
                if(value != null) {
                    data.put(toColumn, DigestUtils.md5Hex((byte[]) value));
                }
                if(isRemoveFile) {
                    data.remove(column);
                }
            }
        }
        send(event);
    }

}
