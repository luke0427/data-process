package com.ropeok.dataprocess.v2.component.trans;

import com.ropeok.dataprocess.utils.Constants;
import com.ropeok.dataprocess.v2.component.AbstractComponent;
import com.ropeok.dataprocess.v2.core.SendEvent;
import org.apache.commons.codec.digest.DigestUtils;

import java.util.Collection;
import java.util.Map;

public class PKMD5Trans extends AbstractComponent {

    private String[] columns;
    private Collection<Map<String, Object>> datas;
    private StringBuilder value = new StringBuilder();
    private String toColumn;
    private String prefix;

    @Override
    public void init() throws Exception {
        this.columns = getStepMeta().getStringPropertyToArray(Constants.COLUMNS);
        this.toColumn = getStepMeta().getStringProperty(Constants.TO_COLUMN);
        this.prefix = getStepMeta().getStringProperty(Constants.PREFIX);
    }

    @Override
    public void exec(SendEvent event) throws Exception {
        if(event != null) {
            datas = event.getDataSet().getData();
            for(Map<String, Object> data : datas) {
                value.setLength(0);
                for(String column : columns) {
                    value.append(data.get(column).toString());
                }
                if(this.prefix != null) {
                    data.put(toColumn, this.prefix + DigestUtils.md5Hex(value.toString()).toUpperCase());
                } else {
                    data.put(toColumn, DigestUtils.md5Hex(value.toString()).toUpperCase());
                }
            }
        }
        send(event);
    }
}
