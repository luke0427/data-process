package com.ropeok.dataprocess.v2.core;

import java.io.Serializable;
import java.util.Collection;

/**
 * 数据接口，持有具体的数据
 */
public interface IDataSet<T> extends Serializable {
    public void addData(T data);
    public Collection<T> getData();
}
