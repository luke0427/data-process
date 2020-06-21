package com.ropeok.dataprocess.config;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.xpack.client.PreBuiltXPackTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;

import java.net.InetAddress;
import java.net.UnknownHostException;

//@Configuration
public class EsConfig {

    private static final Logger LOG = LoggerFactory.getLogger(EsConfig.class);

    @Value("${elasticsearch.server.host}")
    private String host;
    @Value("${elasticsearch.server.port}")
    private int port;
    @Value("${elasticsearch.server.name}")
    private String esServerName;

    @Bean(name="transportClient")
    public TransportClient elasticsearchClient(){
        TransportClient transportClient = null;
        Settings settings = Settings.builder().put("cluster.name", esServerName).put("xpack.security.user", "elastic:elastic").build();
        LOG.info("host:{}, port:{}, name:{}", host, port, esServerName);
        try {
//            transportClient = new PreBuiltXPackTransportClient(settings)
//                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host), port));
            transportClient = new PreBuiltXPackTransportClient(settings)
                    .addTransportAddress(new TransportAddress(InetAddress.getByName(host), port));
        } catch (UnknownHostException e) {
            LOG.error("创建elasticsearch客户端失败");
        }
        LOG.info("创建elasticsearch客户端成功");
        return transportClient;
    }

}
