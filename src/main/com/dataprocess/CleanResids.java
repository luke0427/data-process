package com.ropeok.dataprocess;

import com.ropeok.dataprocess.utils.DBUtils;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPoolConfig;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class CleanResids {

    public static void main(String[] args) throws Exception {
//        insertRedis();
        /*String sfzh = "360731198609022914";
        System.out.println(getValue(sfzh, "ryid"));*/
//        getKey("522526198206030012");
//        getKey("350628197311171036");
        String ryid = "D00T002R0006F1C3270B3B20C920D52EEBE44AF9";
//        getKey("D00T002R20170505153608202248");
        System.out.println(getValue(ryid, "zjhm"));
        System.out.println(getValue(ryid, "xm"));
        System.out.println(getValue(ryid, "xb"));
        System.out.println(getValue(ryid, "csrq"));
        System.out.println(getValue(ryid, "zjlx"));
    }

    public static void getKey(String key) {
        String k = "p:c:"+(key.hashCode()%100);
        System.out.println("key: " + k);
    }

    public static String getValue(String key, String field) {
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(1);
        poolConfig.setMaxIdle(1);
        poolConfig.setMaxWaitMillis(30000);

        Set<HostAndPort> nodes = new LinkedHashSet<HostAndPort>();
        nodes.add(new HostAndPort("35.48.98.47", 7000));
        nodes.add(new HostAndPort("35.48.98.47", 7001));
        nodes.add(new HostAndPort("35.48.98.47", 7002));
        final JedisCluster cluster = new JedisCluster(nodes,5000, 5000, 5, "redis2017@", poolConfig);
        System.out.println("key: " + (key.hashCode() % 100));
        return cluster.hget("p:c:" + (key.hashCode() % 100), key + field);
    }

    public static void insertRedis() throws Exception {
        Connection conn = DBUtils.getConnection("jdbc:oracle:thin:@35.48.98.201:1521:orcl", "rksys", "Ropeok!@#0810");
        PreparedStatement ps = conn.prepareStatement("SELECT idcard,person_id from t_base_person_info", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        ps.setFetchSize(5000);
        ResultSet rs = ps.executeQuery();
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(1);
        poolConfig.setMaxIdle(1);
        poolConfig.setMaxWaitMillis(30000);

        Set<HostAndPort> nodes = new LinkedHashSet<HostAndPort>();
        nodes.add(new HostAndPort("35.48.98.47", 7000));
        nodes.add(new HostAndPort("35.48.98.47", 7001));
        nodes.add(new HostAndPort("35.48.98.47", 7002));
        final JedisCluster cluster = new JedisCluster(nodes,5000, 5000, 5, "redis2017@", poolConfig);

        String idcard = null;
        String pid = null;
        int count = 0;
        while(rs.next()) {
            idcard = rs.getString("IDCARD");
            pid = rs.getString("PERSON_ID");
            cluster.hset("p:c:" + idcard.substring(0,2), idcard.substring(2)+"ryid", pid);
            count++;
            if(count % 5000 == 0 ) {
                System.out.println("已缓存" + count);
            }
        }
        cluster.close();
        rs.close();
        ps.close();
        conn.close();
    }

    public static void cleanRedis() throws SQLException {
        Connection conn = DBUtils.getConnection("jdbc:oracle:thin:@35.48.98.201:1521:orcl", "rksys", "Ropeok!@#0810");
        PreparedStatement ps = conn.prepareStatement("SELECT idcard from t_base_person_info", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        ps.setFetchSize(5000);
        ResultSet rs = ps.executeQuery();
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(1);
        poolConfig.setMaxIdle(1);
        poolConfig.setMaxWaitMillis(30000);

        Set<HostAndPort> nodes = new LinkedHashSet<HostAndPort>();
        nodes.add(new HostAndPort("35.48.98.207", 7000));
        nodes.add(new HostAndPort("35.48.98.207", 7001));
        nodes.add(new HostAndPort("35.48.98.207", 7002));
        final JedisCluster cluster = new JedisCluster(nodes,5000, 5000, 5, "redis2017@", poolConfig);

        String idcard = null;
        int count = 0;
        List<String> keys  = new LinkedList<>();
        ExecutorService service = Executors.newFixedThreadPool(3);

        while(rs.next()) {
            idcard = rs.getString("IDCARD");
            keys.add("proc:ryxx:" + idcard);
            count++;
            if(count % 10000 == 0) {
                final String[] a = new String[keys.size()];
                keys.toArray(a);
                service.submit(new Runnable() {
                    @Override
                    public void run() {
                        for(String v : a) {
                            cluster.del(v);
                        }
                        System.out.println(Thread.currentThread().getName()+ ", 删除数据 "+ a.length);
                    }
                });
                System.out.println("已经清理：" + count);
                keys.clear();
            }
        }
        if(keys.size() > 0) {
            final String[] a = new String[keys.size()];
            keys.toArray(a);
            service.submit(new Runnable() {
                @Override
                public void run() {
                    for(String v : a) {
                        cluster.del(v);
                    }
                    System.out.println(Thread.currentThread().getName()+ ", 删除数据 "+ a.length);
                }
            });
        }
        service.shutdown();
    }
}
