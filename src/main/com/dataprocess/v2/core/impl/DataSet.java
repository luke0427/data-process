package com.ropeok.dataprocess.v2.core.impl;

import com.ropeok.dataprocess.v2.core.IDataSet;

import java.util.*;

public class DataSet implements IDataSet<Map<String, Object>> {

    private List<Map<String, Object>> datas = new LinkedList<>();

    @Override
    public void addData(Map<String, Object> data) {
        datas.add(data);
    }

//    public void addDatas(List<Map<String, Object>> datas) {
//        datas.addAll(datas);
//    }

    @Override
    public Collection<Map<String, Object>> getData() {
        return datas;
    }

}
