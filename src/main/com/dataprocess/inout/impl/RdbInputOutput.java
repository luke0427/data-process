package com.ropeok.dataprocess.inout.impl;

import com.ropeok.dataprocess.inout.InputOutputType;

public class RdbInputOutput extends AbstractInputOutput {
    private String url;
    private String username;
    private String password;
    private String sql;

    public RdbInputOutput() {
        super.type = InputOutputType.JDBC;
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
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
}
