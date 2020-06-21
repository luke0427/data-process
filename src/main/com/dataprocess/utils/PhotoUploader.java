package com.ropeok.dataprocess.utils;

import com.jcraft.jsch.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.Vector;

public class PhotoUploader {

    private static final Logger LOGGER = LoggerFactory.getLogger(PhotoUploader.class);

    private JSch jSch;
    private Session session = null;
    private ChannelSftp channel = null;
    private String path;
    private String directory;
    private Set<String> directories = new LinkedHashSet<>();

    public PhotoUploader(String host, int port, String username, String password, String prefixPath, String directory) {
         jSch = new JSch();
        try {
            this.path = prefixPath + "/" + directory;
            this.directory = directory;
            LOGGER.info("directory={}", directory);
            session = jSch.getSession(username, host, port);
            session.setPassword(password);
            session.setConfig("StrictHostKeyChecking", "no");
            session.connect(30000);
            channel = (ChannelSftp) session.openChannel("sftp");
            channel.connect();
            LOGGER.info("host={}连接成功", host);
            initPath(prefixPath);
        } catch (JSchException e) {
            e.printStackTrace();
            closeAll();
            throw new RuntimeException("连接服务器异常: " + e.getLocalizedMessage());
        } catch (SftpException e) {
            e.printStackTrace();
            closeAll();
            throw new RuntimeException("执行异常: " + e.getLocalizedMessage());
        }
    }

    private void initPath(String prefixPath) throws SftpException {
        Vector<ChannelSftp.LsEntry> files = channel.ls(prefixPath);
        Set<String> dirs = new LinkedHashSet<>();
        for(ChannelSftp.LsEntry file : files) {
            if(file.getAttrs().isDir()) {
                dirs.add(file.getFilename());
            }
        }
        if(!dirs.contains(directory)) {
            LOGGER.info("目录{}不存在，创建", directory);
            channel.mkdir(path);
        } else {
            LOGGER.info("目录{}已存在", directory);
            files = channel.ls(path);
            for(ChannelSftp.LsEntry file : files) {
                if(file.getAttrs().isDir()) {
                    directories.add(file.getFilename());
                }
            }
        }
    }

    public void put(InputStream inputStream, String name) throws SftpException {
        channel.put(inputStream, path + "/" + name);
    }

    private String paramPath;

    public void put(InputStream inputStream, String sufixPath, String name) throws SftpException {
        //判断目录是否存在
        paramPath = path + "/" + sufixPath;
        if(!directories.contains(paramPath)) {
            try {
                channel.mkdir(paramPath);
                LOGGER.info("创建目录{}", paramPath);
            } catch (SftpException e) {
            }
            directories.add(paramPath);
        }
        channel.put(inputStream, paramPath + "/" + name);
    }

    public void put(String src, String name) throws SftpException {
        channel.put(src, path + "/" + name);
    }

    public void closeAll() {
        if(channel != null) {
            channel.disconnect();
        }
        if(session != null) {
            session.disconnect();
        }
    }
}
