package com.ropeok.dataprocess.inout.output;

import com.alibaba.fastjson.JSONArray;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPoolConfig;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class RedisProcOutput extends AbstractProcOutput{

    private static final Logger LOGGER = LoggerFactory.getLogger(RedisProcOutput.class);
//    private Jedis jedis = null;
    private JedisCluster cluster = null;
    private List<String> keyColumns = null;
    private List<String> valueColumns = null;
//    private Map<String, String> data = new HashMap<>();
    private String keyPrefix = null;
    private StringBuilder key;
    private long cacheRow = 0;
    public final static int SEGMENT = 100;
    private int expire = 3600;

    @Override
    public void init() {
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(1);
        poolConfig.setMaxIdle(1);
        poolConfig.setMaxWaitMillis(30000);
        Set<HostAndPort> nodes = new LinkedHashSet<HostAndPort>();

        List<String> serverips = ((JSONArray)procOutputMeta.getParam("serverips")).toJavaList(String.class);
        List<Integer> serverports = ((JSONArray)procOutputMeta.getParam("serverports")).toJavaList(Integer.class);
        for(int i= 0,length = serverips.size(); i < length; i++) {
            nodes.add(new HostAndPort(serverips.get(i), serverports.get(i)));
        }
//        nodes.add(new HostAndPort(procOutputMeta.getServerip(), procOutputMeta.getServerport()));
//        jedis = new Jedis(procOutputMeta.getServerip(), procOutputMeta.getServerport());
        String pwd = procOutputMeta.getPassword();
        if(StringUtils.isNotBlank(pwd)) {
//            jedis.auth(pwd);
            cluster = new JedisCluster(nodes, 3000, 3000, 5, pwd, poolConfig);
        } else {
            cluster = new JedisCluster(nodes, poolConfig);
        }

        keyColumns = ((JSONArray)procOutputMeta.getParam("keyColumns")).toJavaList(String.class);
        valueColumns = ((JSONArray)procOutputMeta.getParam("valueColumns")).toJavaList(String.class);//TODO : 防止类变量数量过多，后面改成用getParam获取数据
        keyPrefix = (String) procOutputMeta.getParam("keyPrefix");
        key = new StringBuilder();
        expire = procOutputMeta.getParam("expire") == null ? expire : Integer.parseInt(String.valueOf(procOutputMeta.getParam("expire")));
        LOGGER.info("缓存生存时间{}秒", expire);
    }

    @Override
    protected void exec(Map<String, Object> row) throws Exception {
        key.setLength(0);
        if(row.size() > 0) {
            cacheRow++;
            for(String column : keyColumns) {
                key.append(row.get(column));
            }

            for(String value : valueColumns) {
                cluster.hset((keyPrefix + (key.toString().hashCode() % SEGMENT)), (key.toString()+value), row.get(value) == null ? "" : (String) row.get(value));
            }

            cluster.expire(key.toString(), expire);

            if(cacheRow % 5000 == 0) {
                long t = stopWatch.getTime();
                LOGGER.info("已缓存数据{}条，共耗时{}ms,速度:{}条/秒", cacheRow, t, (int)((((double)cacheRow)*1000)/t));
            }
        }
    }

    @Override
    public void finished() throws Exception {
        this.cluster.close();
    }

}
