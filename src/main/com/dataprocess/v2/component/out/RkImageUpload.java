package com.ropeok.dataprocess.v2.component.out;

import com.ropeok.dataprocess.utils.Constants;
import com.ropeok.dataprocess.utils.PhotoUploader;
import com.ropeok.dataprocess.v2.component.AbstractComponent;
import com.ropeok.dataprocess.v2.core.SendEvent;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.util.Collection;
import java.util.Map;

public class RkImageUpload extends AbstractComponent {

    private static final Logger LOGGER = LoggerFactory.getLogger(RkImageUpload.class);
    private PhotoUploader photoUploader;
    private Collection<Map<String, Object>> datas;
    private String column;
    private String nameColum;
    private String paramColumn;
    private Object object;
    private boolean isThrow;
    private String paramValue;
    private int batchSize;

    @Override
    public void init() throws Exception {
        String host = getStepMeta().getStringProperty(Constants.HOST);
        int port = getStepMeta().getIntProperty(Constants.PORT);
        String username = getStepMeta().getStringProperty(Constants.USER_NAME);
        String password = getStepMeta().getStringProperty(Constants.PASSWORD);
        String prefixPath = getStepMeta().getStringProperty(Constants.UPLOAD_PATH);
        String dir = getStepMeta().getStringProperty(Constants.UPLOAD_DIR);
        this.column = getStepMeta().getStringProperty(Constants.COLUMN);
        this.nameColum = getStepMeta().getStringProperty(Constants.NAME_COLUMN);
        this.isThrow = getStepMeta().getBooleanProperty(Constants.THROW);
        this.paramColumn = getStepMeta().getStringProperty(Constants.PARAM_COLUMN);
        this.batchSize = Integer.parseInt(getStepMeta().getOrDefaultStringProperty(Constants.BATCH_SIZE, "500"));

        photoUploader = new PhotoUploader(host, port, username, password, prefixPath, dir);
    }

    @Override
    public void exec(SendEvent event) throws Exception {
        if(event != null) {
            datas = event.getDataSet().getData();
            for(Map<String, Object> data : datas) {
                object = data.get(column);
                this.paramValue = (String) data.get(paramColumn);
                if(object instanceof byte[]) {
                    if(StringUtils.isNotBlank(paramValue)) {
                        photoUploader.put(new ByteArrayInputStream((byte[]) object), paramValue, String.valueOf(data.get(this.nameColum)) + ".jpg");
                    } else {
                        photoUploader.put(new ByteArrayInputStream((byte[]) object), String.valueOf(data.get(this.nameColum)) + ".jpg");
                    }
                }
                if(isThrow) { //只是单纯移除该字段，并不会丢弃数据
                    data.remove(column);
                }
            }
            printStatInfo();
        }
        send(event);
    }

    private void printStatInfo() {
        if (stats.getCurrentRow() >= batchSize) {
            LOGGER.info("{}", stats.getCurrentInfo());
            stats.reset();
        }
    }

    @Override
    public void finished() throws Exception {
        super.finished();
        photoUploader.closeAll();
    }
}
