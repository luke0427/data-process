package com.ropeok.dataprocess.meta;

import com.ropeok.dataprocess.meta.ColumnMeta;

import java.util.List;

public class RowMeta {

    private String type;

    private String tocolumn;

    private String format;

    public RowMeta(){}

    public RowMeta(String type) {
        this.type = type;
    }

    private List<ColumnMeta> columnMetas;

    public List<ColumnMeta> getColumnMetas() {
        return columnMetas;
    }

    public void setColumnMetas(List<ColumnMeta> columnMetas) {
        this.columnMetas = columnMetas;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getTocolumn() {
        return tocolumn;
    }

    public void setTocolumn(String tocolumn) {
        this.tocolumn = tocolumn;
    }

    public String getFormat() {
        return format;
    }

    public void setFormat(String format) {
        this.format = format;
    }
}
