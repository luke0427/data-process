package com.ropeok.dataprocess.inout.impl;

import com.ropeok.dataprocess.inout.InputOutputType;

public class ESInputOutput extends AbstractInputOutput{
    private String serverip;
    private String servername;
    private int serverport;
    private String username;
    private String password;
    private String index;
    private String idxtype;
    private String idName;

    public String getIdName() {
        return idName;
    }

    public void setIdName(String idName) {
        this.idName = idName;
    }

    public ESInputOutput() {
        super.type = InputOutputType.ES;
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
}
