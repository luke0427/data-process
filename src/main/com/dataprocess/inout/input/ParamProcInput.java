package com.ropeok.dataprocess.inout.input;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class ParamProcInput extends AbstractProcInput {

    public static final String DATA_KEY = "DATA";
    private Iterator<Map<String, Object>> iterator;

    @Override
    public boolean hasNext() throws Exception {
        return iterator == null ? false : iterator.hasNext();
    }

    @Override
    public Map<String, Object> next() throws Exception {
        return iterator.next();
    }

    @Override
    public void init() throws Exception {
        List<Map<String, Object>> values = JSONObject.parseObject(jobDetail.getJobDataMap().get(ParamProcInput.DATA_KEY).toString(), new TypeReference<List<Map<String, Object>>>(){});
        if(values != null) {
            iterator = values.iterator();
        }
    }

    @Override
    public void finished() throws Exception {
        iterator = null;
    }

    @Override
    public void onError() throws Exception {
    }
}
