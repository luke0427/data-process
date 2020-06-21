package com.ropeok.dataprocess.handler.impl;

import com.ropeok.dataprocess.handler.HandleStatus;
import com.ropeok.dataprocess.meta.ColumnMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPoolConfig;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class RedisProcHandler extends AbstractProcHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(RedisProcHandler.class);
//    private Jedis jedis = null;
    private JedisCluster cluster = null;
    private String[] columnNames;
    private List<String> columnValues;
    private ColumnMeta columnMeta = null;
    private String value = null;
    public static final int SEGMENT = 100;

    @Override
    public void init() {
        super.init();
        LOGGER.info("开始初始化Redis连接");
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(1);
        poolConfig.setMaxIdle(1);
        poolConfig.setMaxWaitMillis(30000);
        Set<HostAndPort> nodes = new LinkedHashSet<HostAndPort>();
        nodes.add(new HostAndPort(procHandlerMeta.getServerip(), procHandlerMeta.getServerport()));

//        jedis = new Jedis(procHandlerMeta.getServerip(), procHandlerMeta.getServerport());
        if (procHandlerMeta.getPassword() != null) {
//            jedis.auth(procHandlerMeta.getPassword());
            cluster = new JedisCluster(nodes, 5000, 5000, 5, procHandlerMeta.getPassword(), poolConfig);
        } else {
            cluster = new JedisCluster(nodes, poolConfig);
        }
    }

    @Override
    public HandleStatus handle(Map<String, Object> data) throws Exception {

        columnNames = new String[procHandlerMeta.getColumnMetas().size()];
        for (int i = 0, length = columnNames.length; i < length; i++) {
            columnNames[i] = String.valueOf(procHandlerMeta.getColumnMetas().get(i).getColumnName());
        }

        for (int i = 0, length = columnNames.length; i < length; i++) {
            columnMeta = procHandlerMeta.getColumnMetas().get(i);
            value = cluster.hget((procHandlerMeta.getKeyPrefix() + (data.get(procHandlerMeta.getKeyColumn()).toString().hashCode() % SEGMENT)), data.get(procHandlerMeta.getKeyColumn()).toString() + columnMeta.getColumnName());
            if (value != null) {
                data.put(columnMeta.getToName(), value);
            }
        }

        if(nextHandler != null) {
            return nextHandler.handle(data);
        }
        return HandleStatus.SUCC;
    }
}
