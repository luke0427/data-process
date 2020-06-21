package com.ropeok.dataprocess.inout.input;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.search.ClearScrollRequestBuilder;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.elasticsearch.xpack.client.PreBuiltXPackTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Iterator;
import java.util.Map;

public class ESProcInput extends AbstractProcInput{

    private final static Logger LOGGER = LoggerFactory.getLogger(ESProcInput.class);

    private TransportClient transportClient;
    private SearchRequestBuilder searchRequestBuilder;
    private SearchResponse searchResponse;
    private long totalHits = 0L;
    private int fetchHits = 0;
    private int currentIdx = 0;
    private long currentTotalIdx = 0L;
    private Iterator<SearchHit> hitIterator = null;
    private QueryBuilder queryBuilder = null;
    private String[] includesSource = null;

    @Override
    public void init() throws Exception {
        Settings.Builder settingsBuilder = Settings.builder().put("cluster.name", procInputMeta.getServername());
        if(!StringUtils.isBlank(procInputMeta.getUsername())) {
            settingsBuilder.put("xpack.security.user", procInputMeta.getUsername() + ":" + procInputMeta.getPassword());
        }
        LOGGER.info("host:{}, port:{}, name:{}", procInputMeta.getServerip(), procInputMeta.getServerport(), procInputMeta.getServername());
        Settings settings = settingsBuilder.build();
        try {
            if(!StringUtils.isBlank(procInputMeta.getUsername())) {
//                transportClient = new PreBuiltXPackTransportClient(settings)
//                        .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(procInputMeta.getServerip()), procInputMeta.getServerport()));
                transportClient = new PreBuiltXPackTransportClient(settings)
                        .addTransportAddress(new TransportAddress(InetAddress.getByName(procInputMeta.getServerip()), procInputMeta.getServerport()));
            } else {
//                transportClient = new PreBuiltTransportClient(settings).addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(procInputMeta.getServerip()), procInputMeta.getServerport()));
                transportClient = new PreBuiltTransportClient(settings).addTransportAddress(new TransportAddress(InetAddress.getByName(procInputMeta.getServerip()), procInputMeta.getServerport()));
            }
        } catch (UnknownHostException e) {
            LOGGER.error("ES输入客户端创建失败");
        }
        LOGGER.info("ES输入客户端创建成功");
        if(StringUtils.isNotBlank(procInputMeta.getSql())) {
            LOGGER.info("Query={}", procInputMeta.getSql());
            JSONObject jsonObject = JSONObject.parseObject(procInputMeta.getSql());
            queryBuilder = QueryBuilders.wrapperQuery(jsonObject.getString("query"));
            JSONObject sourceObject = jsonObject.getJSONObject("_source");
            if(sourceObject != null) {
                JSONArray includes = sourceObject.getJSONArray("includes");
                includesSource = new String[includes.size()];
                includes.toArray(includesSource);
            }
        }

        fetchData();
    }

    protected void fetchData() {
        if(searchResponse != null) {
            searchResponse = transportClient.prepareSearchScroll(searchResponse.getScrollId()).setScroll(TimeValue.timeValueMinutes(10)).execute().actionGet();
        } else {
            searchRequestBuilder = transportClient.prepareSearch(procInputMeta.getIndex()).setTypes(procInputMeta.getIdxtype()).setSize(procInputMeta.getFetchSize());
            if(queryBuilder != null) {
                searchRequestBuilder.setQuery(queryBuilder);
            }
            if(includesSource != null && includesSource.length > 0) {
                searchRequestBuilder.setFetchSource(includesSource, null);//比较少用excludes，所以这里先简单处理
            }
            searchResponse = searchRequestBuilder.setScroll(TimeValue.timeValueMinutes(10)).execute().actionGet();
            totalHits = searchResponse.getHits().getTotalHits();
        }
        hitIterator = searchResponse.getHits().iterator();
        fetchHits = searchResponse.getHits().getHits().length;
        currentIdx = 0;
    }

    @Override
    public boolean hasNext() throws Exception {
//        LOGGER.info("currentIdx:{}, currentTotalIdx:{}, fetchHits:{}, total:{}", currentIdx, currentTotalIdx, fetchHits, totalHits);
        return hitIterator.hasNext() || totalHits > currentTotalIdx;
    }

    @Override
    public Map<String, Object> next() throws Exception {
        if(currentIdx >= fetchHits && currentTotalIdx < totalHits) {
            fetchData();
        }
        currentIdx++;
        currentTotalIdx++;
        Map<String, Object> row = hitIterator.next().getSourceAsMap();
//        LOGGER.info("row:{}", row);
        return row;
    }

    @Override
    public void finished() throws Exception {
        LOGGER.info("执行结束工作");
        try {
            ClearScrollRequestBuilder clearScrollRequestBuilder = transportClient.prepareClearScroll();
            clearScrollRequestBuilder.addScrollId(searchResponse.getScrollId());
            LOGGER.info("ClearScroll result={}", clearScrollRequestBuilder.get().isSucceeded());
        } finally {
            transportClient.close();
        }
    }

    @Override
    public void onError() throws Exception {
    }
}
