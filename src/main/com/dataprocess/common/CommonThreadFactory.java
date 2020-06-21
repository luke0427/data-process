package com.ropeok.dataprocess.common;

import java.util.concurrent.ThreadFactory;

public class CommonThreadFactory implements ThreadFactory {

    private int count = 0;
    private String name;

    public CommonThreadFactory(String name) {
        this.name = name;
    }
    @Override
    public Thread newThread(Runnable r) {
        return new Thread(r, name + "-" + ++count);
    }
}
