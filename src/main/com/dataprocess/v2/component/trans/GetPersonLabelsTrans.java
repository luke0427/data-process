package com.ropeok.dataprocess.v2.component.trans;

import com.google.common.base.Preconditions;
import com.ropeok.dataprocess.utils.Constants;
import com.ropeok.dataprocess.utils.DBUtils;
import com.ropeok.dataprocess.v2.component.AbstractComponent;
import com.ropeok.dataprocess.v2.core.SendEvent;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPoolConfig;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.*;

/**
 * 定制化：获取人员标签列表
 */
public class GetPersonLabelsTrans extends AbstractComponent {

    private static final Logger LOGGER = LoggerFactory.getLogger(GetPersonLabelsTrans.class);
    private JedisCluster cluster;
    private Jedis jedis;
    private String keyColumn;
    private String keyValue;
    private String toColumn;
    private Collection<Map<String, Object>> datas;
    private Set<String> labels;
    private Set<String> personLabels;
    private StringBuilder str = new StringBuilder();

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
        keyColumn = getStepMeta().getStringProperty(Constants.KEY_COLUMN);
        toColumn = getStepMeta().getStringProperty(Constants.TO_COLUMN);
        String labelsUrl = getStepMeta().getStringProperty("labels_url");
        String labelsUsername = getStepMeta().getStringProperty("labels_username");
        String labelsPassword = getStepMeta().getStringProperty("labels_password");
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            conn = DBUtils.getConnection(labelsUrl, labelsUsername, labelsPassword);
            ps = conn.prepareStatement("select t.big_tag || '|' || t.small_tag \"label\"\n" +
                    "  from t_labels_dictionary t\n" +
                    " group by t.big_tag, t.small_tag", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            ps.setFetchSize(5000);
            rs = ps.executeQuery();
            labels = new LinkedHashSet<>();
            while(rs.next()) {
                labels.add(rs.getString("label"));
            }
        } finally {
            DBUtils.close(rs);
            DBUtils.close(ps);
            DBUtils.close(conn);
        }

    }

    @Override
    public void exec(SendEvent event) throws Exception {
        if(event != null) {
            datas = event.getDataSet().getData();
            for(Map<String, Object> data : datas) {
                keyValue = (String) data.get(keyColumn);
                if(keyValue != null) {
                    str.setLength(0);
                    for(String lb : labels) {
                        if(jedis != null && jedis.sismember(lb, keyValue)) {
                            str.append(lb).append(",");
                        } else if(cluster != null && cluster.sismember(lb, keyValue)) {
                            str.append(lb).append(",");
                        }
                    }
                    if(str != null && str.length() > 0) {
                        data.put(toColumn, str.substring(0, str.length()-1));
                    }
                }
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
