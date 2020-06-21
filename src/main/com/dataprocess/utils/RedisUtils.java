package com.ropeok.dataprocess.utils;

public class RedisUtils {

    /**
     * Redis 的分段数
     */
    public static final int SEGMENT = 100;

    public static int getSegmentNo(String value) {
        return value.hashCode() % SEGMENT;
    }
}
