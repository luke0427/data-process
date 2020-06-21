package com.ropeok.dataprocess.meta;

import com.ropeok.dataprocess.inout.ProcInputType;
import com.ropeok.dataprocess.inout.ProcOutputType;

import java.util.HashMap;
import java.util.Map;

public class ProcInputOutputMeta {
    private ProcInputType procInputType;
    private IncreMeta increMeta;
    private String url;
    private String username;
    private String password;
    private int fetchSize;
    private String sql;

    private ProcOutputType procOutputType;
    private String serverip;
    private String servername;
    private int serverport;
    /*private String[] serverips;
    private int[] serverports;*/
    private String index;
    private String idxtype;
    private String idName;
    private int batchSize;
    private int poolSize;
    private String[] keyColumns;
    private String[] valueColumns;
    private Map<String, Object> properties = new HashMap<>();

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

    public boolean exist(String key) {
        return this.properties.containsKey(key);
    }

    public String[] getValueColumns() {
        return valueColumns;
    }

    public void setValueColumns(String[] valueColumns) {
        this.valueColumns = valueColumns;
    }

    public String[] getKeyColumns() {
        return keyColumns;
    }

    public void setKeyColumns(String[] keyColumns) {
        this.keyColumns = keyColumns;
    }

    public int getPoolSize() {
        return poolSize;
    }

    public void setPoolSize(int poolSize) {
        this.poolSize = poolSize;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public ProcOutputType getProcOutputType() {
        return procOutputType;
    }

    public void setProcOutputType(ProcOutputType procOutputType) {
        this.procOutputType = procOutputType;
    }

    public String getServerip() {
        return serverip;
    }

    public void setServerip(String serverip) {
        this.serverip = serverip;
    }

    public String getServername() {
        return servername;
    }

    public void setServername(String servername) {
        this.servername = servername;
    }

    public int getServerport() {
        return serverport;
    }

    public void setServerport(int serverport) {
        this.serverport = serverport;
    }

    public String getIndex() {
        return index;
    }

    public void setIndex(String index) {
        this.index = index;
    }

    public String getIdxtype() {
        return idxtype;
    }

    public void setIdxtype(String idxtype) {
        this.idxtype = idxtype;
    }

    public String getIdName() {
        return idName;
    }

    public void setIdName(String idName) {
        this.idName = idName;
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public ProcInputType getProcInputType() {
        return procInputType;
    }

    public void setProcInputType(ProcInputType procInputType) {
        this.procInputType = procInputType;
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

    public int getFetchSize() {
        return fetchSize == 0 ? 5000 : fetchSize;
    }

    public void setFetchSize(int fetchSize) {
        this.fetchSize = fetchSize;
    }

    public IncreMeta getIncreMeta() {
        return increMeta;
    }

    public void setIncreMeta(IncreMeta increMeta) {
        this.increMeta = increMeta;
    }
}
