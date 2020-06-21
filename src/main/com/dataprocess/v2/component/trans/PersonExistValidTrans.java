package com.ropeok.dataprocess.v2.component.trans;

import com.google.common.base.Preconditions;
import com.ropeok.dataprocess.utils.Constants;
import com.ropeok.dataprocess.utils.RedisUtils;
import com.ropeok.dataprocess.v2.component.AbstractComponent;
import com.ropeok.dataprocess.v2.core.SendEvent;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPoolConfig;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

public class PersonExistValidTrans extends AbstractComponent {

    private static final Logger LOGGER = LoggerFactory.getLogger(PersonExistValidTrans.class);
    private JedisCluster cluster;
    private Collection<Map<String, Object>> datas;
    private String keyColumn;
    private String cacheColumn;
    private String toColumn;
    private String cacheToColumn;
    private String prefix;
    private Object value;
    private String cacheValue;
    private static final String EXIST = "1";
    private static final String NOT_EXIST = "0";

    @Override
    public void init() throws Exception {
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(1);
        poolConfig.setMaxIdle(1);
        poolConfig.setMaxWaitMillis(30000);
        Set<HostAndPort> nodes = new LinkedHashSet<HostAndPort>();

        String[] hosts = getStepMeta().getStringPropertyToArray(Constants.HOSTS);
        String[] ports = getStepMeta().getStringPropertyToArray(Constants.PORTS);
        Preconditions.checkArgument(hosts.length == ports.length);

        for(int i= 0,length = hosts.length; i < length; i++) {
            nodes.add(new HostAndPort(hosts[i], Integer.parseInt(ports[i])));
        }

        String pwd = getStepMeta().getStringProperty(Constants.PASSWORD);
        if(StringUtils.isNotBlank(pwd)) {
            cluster = new JedisCluster(nodes, 3000, 3000, 5, pwd, poolConfig);
        } else {
            cluster = new JedisCluster(nodes, poolConfig);
        }
        LOGGER.info("Redis连接初始化完成");
        keyColumn = getStepMeta().getStringProperty(Constants.KEY_COLUMN);
        cacheColumn = getStepMeta().getStringProperty(Constants.CACHE_COLUMN);
        toColumn = getStepMeta().getStringProperty(Constants.TO_COLUMN);
        cacheToColumn = getStepMeta().getStringProperty(Constants.CACHE_TO_COLUMN);
        prefix = getStepMeta().getStringProperty(Constants.PREFIX);
    }

    @Override
    public void exec(SendEvent event) throws Exception {
        if(event != null) {
            datas = event.getDataSet().getData();
            for(Map<String, Object> data : datas) {
                value = data.get(keyColumn);
                if(value == null && toColumn != null) {
                    data.put(toColumn, NOT_EXIST);
                } else {
                    cacheValue = cluster.hget(prefix + RedisUtils.getSegmentNo(String.valueOf(value)), String.valueOf(value) + cacheColumn);
                    if(toColumn != null) {
                        data.put(toColumn, cacheValue != null ? EXIST : NOT_EXIST);
                    }
                    if(cacheToColumn != null) {
                        data.put(cacheToColumn, cacheValue);
                    }
                }
            }
        }
        send(event);
    }

    @Override
    public void finished() throws Exception {
        if(cluster != null) {
            cluster.close();
        }
        super.finished();
    }
}
