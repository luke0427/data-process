package com.ropeok.dataprocess.meta;

import com.ropeok.dataprocess.handler.ProcHandlerType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ProcHandlerMeta {

    private ProcHandlerType procHandlerType = null;

    private String serverip;
    private int serverport;
    private String keyPrefix;
    private String tocolumn;

    private String format;

    private String sql;
    private String url;
    private String username;
    private String password;
    private String keyColumn;
    private String cacheKeyCol;
    private String multiValue;

    private String ignore;

    private String pickBirth;
    private String birthColumn;
    private String pickType;
    private String typeColumn;
    private String pickSex;
    private String sexColumn;

    private String column;
    private List<Object> values;

    private List<ColumnMeta> columnMetas;

    private Map<String, Object> properties = new HashMap<>();

    private ProcHandlerMeta nextProcHandlerMeta;

    public ProcHandlerMeta() {}

    public ProcHandlerMeta(ProcHandlerType procHandlerType) {
        this.procHandlerType = procHandlerType;
    }

    public Map<String, Object> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, Object> properties) {
        this.properties = properties;
    }

    public void setParam(String key, Object value) {
        this.properties.put(key, value);
    }

    public Object getParam(String key) {
        return properties.get(key);
    }

    public boolean existKey(String key) {
        return this.properties.containsKey(key);
    }

    public String getServerip() {
        return serverip;
    }

    public void setServerip(String serverip) {
        this.serverip = serverip;
    }

    public int getServerport() {
        return serverport;
    }

    public void setServerport(int serverport) {
        this.serverport = serverport;
    }

    public String getKeyPrefix() {
        return keyPrefix;
    }

    public void setKeyPrefix(String keyPrefix) {
        this.keyPrefix = keyPrefix;
    }

    public String getColumn() {
        return column;
    }

    public void setColumn(String column) {
        this.column = column;
    }

    public List<Object> getValues() {
        return values;
    }

    public void setValues(List<Object> values) {
        this.values = values;
    }

    public String getPickBirth() {
        return pickBirth;
    }

    public void setPickBirth(String pickBirth) {
        this.pickBirth = pickBirth;
    }

    public String getBirthColumn() {
        return birthColumn;
    }

    public void setBirthColumn(String birthColumn) {
        this.birthColumn = birthColumn;
    }

    public String getPickType() {
        return pickType;
    }

    public void setPickType(String pickType) {
        this.pickType = pickType;
    }

    public String getTypeColumn() {
        return typeColumn;
    }

    public void setTypeColumn(String typeColumn) {
        this.typeColumn = typeColumn;
    }

    public String getPickSex() {
        return pickSex;
    }

    public void setPickSex(String pickSex) {
        this.pickSex = pickSex;
    }

    public String getSexColumn() {
        return sexColumn;
    }

    public void setSexColumn(String sexColumn) {
        this.sexColumn = sexColumn;
    }

    public String getMultiValue() {
        return multiValue;
    }

    public void setMultiValue(String multiValue) {
        this.multiValue = multiValue;
    }

    public ProcHandlerMeta getNextProcHandlerMeta() {
        return nextProcHandlerMeta;
    }

    public void setNextProcHandlerMeta(ProcHandlerMeta nextProcHandlerMeta) {
        this.nextProcHandlerMeta = nextProcHandlerMeta;
    }

    public ProcHandlerType getProcHandlerType() {
        return procHandlerType;
    }

    public void setProcHandlerType(ProcHandlerType procHandlerType) {
        this.procHandlerType = procHandlerType;
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

    public List<ColumnMeta> getColumnMetas() {
        return columnMetas;
    }

    public void setColumnMetas(List<ColumnMeta> columnMetas) {
        this.columnMetas = columnMetas;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getKeyColumn() {
        return keyColumn;
    }

    public void setKeyColumn(String keyColumn) {
        this.keyColumn = keyColumn;
    }

    public String getCacheKeyCol() {
        return cacheKeyCol;
    }

    public void setCacheKeyCol(String cacheKeyCol) {
        this.cacheKeyCol = cacheKeyCol;
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public String getIgnore() {
        return ignore;
    }

    public void setIgnore(String ignore) {
        this.ignore = ignore;
    }
}
