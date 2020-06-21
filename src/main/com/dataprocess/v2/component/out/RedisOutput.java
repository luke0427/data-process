package com.ropeok.dataprocess.v2.component.out;

import com.google.common.base.Preconditions;
import com.ropeok.dataprocess.utils.Constants;
import com.ropeok.dataprocess.utils.RedisUtils;
import com.ropeok.dataprocess.v2.component.AbstractComponent;
import com.ropeok.dataprocess.v2.core.SendEvent;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPoolConfig;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

public class RedisOutput extends AbstractComponent {

    private static final Logger LOGGER = LoggerFactory.getLogger(RedisOutput.class);
    private JedisCluster cluster;
    private Jedis jedis;
    private String cacheType;
    private String keyColumn;
    private String[] cacheColumns;
    private String prefix;
    private Collection<Map<String, Object>> datas;
    private String keyValue;
    private String value;
    private int expire;
    private String hashKey;
    private int batchSize;


    @Override
    public void init() throws Exception {
        String host = getStepMeta().getStringProperty(Constants.HOST);
        String pwd = getStepMeta().getStringProperty(Constants.PASSWORD);
        if(StringUtils.isNotBlank(host)) {
            int port = getStepMeta().getIntProperty(Constants.PORT);
            jedis = new Jedis(host, port);
            if(StringUtils.isNotBlank(pwd)) {
                jedis.auth(pwd);
            }
        } else {
            JedisPoolConfig poolConfig = new JedisPoolConfig();
            poolConfig.setMaxTotal(1);
            poolConfig.setMaxIdle(1);
            poolConfig.setMaxWaitMillis(30000);
            Set<HostAndPort> nodes = new LinkedHashSet<HostAndPort>();

            String[] hosts = getStepMeta().getStringPropertyToArray(Constants.HOSTS);
            String[] ports = getStepMeta().getStringPropertyToArray(Constants.PORTS);
            Preconditions.checkNotNull(hosts);
            Preconditions.checkNotNull(ports);
            Preconditions.checkArgument(hosts.length == ports.length);

            for(int i= 0,length = hosts.length; i < length; i++) {
                nodes.add(new HostAndPort(hosts[i], Integer.parseInt(ports[i])));
            }

            if(StringUtils.isNotBlank(pwd)) {
                cluster = new JedisCluster(nodes, 3000, 3000, 5, pwd, poolConfig);
            } else {
                cluster = new JedisCluster(nodes, poolConfig);
            }
        }
        LOGGER.info("Redis连接初始化完成");
        cacheType = getStepMeta().getStringProperty(Constants.CACHE_TYPE);
        keyColumn = getStepMeta().getStringProperty(Constants.KEY_COLUMN);
        cacheColumns = getStepMeta().getStringPropertyToArray(Constants.CACHE_COLUMNS);
        prefix = getStepMeta().getOrDefaultStringProperty(Constants.PREFIX, "");
        expire = getStepMeta().getOrDefaultIntProperty(Constants.EXPIRE, -1);
        batchSize = Integer.parseInt(getStepMeta().getOrDefaultStringProperty(Constants.BATCH_SIZE, "50000"));
    }

    @Override
    public void exec(SendEvent event) throws Exception {
        if(event != null) {
            datas = event.getDataSet().getData();
            for(Map<String, Object> data : datas) {
                keyValue = (String) data.get(keyColumn);
                if(keyValue != null && Constants.CACHE_TYPE_HASH.equals(cacheType)) {
                    for(String cacheColumn : cacheColumns) {
                        value = (String) data.get(cacheColumn);
                        hashKey = prefix + RedisUtils.getSegmentNo(keyValue);
                        if(jedis != null && value != null) {
                            jedis.hset(hashKey, keyValue + cacheColumn, value);
                            if(expire != -1) {
                                jedis.expire(hashKey, expire);
                            }
                        } else {
                            if(value != null) {
                                cluster.hset(hashKey, keyValue + cacheColumn, value != null ? value : "");
                                if(expire != -1) {
                                    cluster.expire(hashKey, expire);
                                }
                            }
                        }
                    }
                } else if(keyValue != null && Constants.CACHE_TYPE_SET.equals(cacheType)) {
                    for(String cacheColumn : cacheColumns) {
                        value = (String) data.get(cacheColumn);
                        if(StringUtils.isNotBlank(value)) {
                            if(jedis != null) {
                                jedis.sadd(keyValue, value);
                            } else {
                                cluster.sadd(keyValue, value);
                            }
                        }
                    }
                    if(expire != -1) {
                        if(jedis != null) {
                            jedis.expire(keyValue, expire);
                        } else {
                            cluster.expire(keyValue, expire);
                        }
                    }
                } else if(keyValue != null && Constants.CACHE_TYPE_LIST.equals(cacheType)) {
                    for(String cacheColumn : cacheColumns) {
                        value = (String) data.get(cacheColumn);
                        if(StringUtils.isNotBlank(value)) {
                            if(jedis != null) {
                                jedis.lpush(keyValue, value);
                            } else {
                                cluster.lpush(keyValue, value);
                            }
                        }
                    }
                    if(expire != -1) {
                        if(jedis != null) {
                            jedis.expire(keyValue, expire);
                        } else {
                            cluster.expire(keyValue, expire);
                        }
                    }
                } else {
                    new RuntimeException("不支持的缓存类型: " + cacheType);
                }
            }
            if (stats.getCurrentRow() >= batchSize) {
                LOGGER.info("{}", stats.getCurrentInfo());
                stats.reset();
            }
        }
        send(event);
    }

    @Override
    public void finished() throws Exception {
        if(jedis != null) {
            jedis.close();
        }
        if(cluster != null) {
            cluster.close();
        }
        super.finished();
    }
}
