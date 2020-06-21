package com.ropeok.dataprocess.inout.output;

import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.elasticsearch.xpack.client.PreBuiltXPackTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;

public class ESProcOutput extends AbstractProcOutput {

    private static final Logger LOGGER = LoggerFactory.getLogger(ESProcOutput.class);
    private TransportClient transportClient;
    private int batchSize;
    private int rowCount = 0;
    private int total = 0;
    private IndexRequestBuilder indexBuilder;
    private UpdateRequestBuilder updateBuilder;
    private boolean isUpsert = false;
    private BulkRequestBuilder bulkRequest;
    private String index;
    private String type;
    private String idName;
    private String routingColumn;
    private String routingValue;
    private String parentColumn;
    private String parentValue;
    private BulkResponse bulkResponse;
    private String[] upcolumns;

    @Override
    public void init() {
        Settings.Builder settingsBuilder = Settings.builder().put("cluster.name", procOutputMeta.getServername());
        if(!StringUtils.isBlank(procOutputMeta.getUsername())) {
            settingsBuilder.put("xpack.security.user", procOutputMeta.getUsername() + ":" + procOutputMeta.getPassword());
        }
        LOGGER.info("host:{}, port:{}, name:{}", procOutputMeta.getServerip(), procOutputMeta.getServerport(), procOutputMeta.getServername());
        Settings settings = settingsBuilder.build();
        try {
            if(!StringUtils.isBlank(procOutputMeta.getUsername())) {
//                transportClient = new PreBuiltXPackTransportClient(settings)
//                        .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(procOutputMeta.getServerip()), procOutputMeta.getServerport()));
                transportClient = new PreBuiltXPackTransportClient(settings)
                        .addTransportAddress(new TransportAddress(InetAddress.getByName(procOutputMeta.getServerip()), procOutputMeta.getServerport()));
            } else {
//                transportClient = new PreBuiltTransportClient(settings).addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(procOutputMeta.getServerip()), procOutputMeta.getServerport()));
                transportClient = new PreBuiltTransportClient(settings).addTransportAddress(new TransportAddress(InetAddress.getByName(procOutputMeta.getServerip()), procOutputMeta.getServerport()));
            }
        } catch (UnknownHostException e) {
            LOGGER.error("ES输出客户端创建失败");
        }
        batchSize = procOutputMeta.getBatchSize();

        this.index = procOutputMeta.getIndex();
        this.type = procOutputMeta.getIdxtype();
        this.idName = procOutputMeta.getIdName();
        this.routingColumn = (String) procOutputMeta.getParam("routingColumn");
        this.parentColumn = (String) procOutputMeta.getParam("parentColumn");
        bulkRequest = transportClient.prepareBulk();
        String mode = (String) procOutputMeta.getParam("mode");
        isUpsert = "upsert".equals(mode);
        if(procOutputMeta.exist("upcolumns")) {
            upcolumns = ((String) procOutputMeta.getParam("upcolumns")).split(",");
        }
        LOGGER.info("ES输出客户端创建成功，isUpsert:{}", isUpsert);
    }

    @Override
    public void finished() throws Exception {
        this.transportClient.close();
    }

    @Override
    protected void exec(Map<String, Object> row) throws Exception {
        if(row.size() > 0) {
            rowCount++;
            total++;

            if (isUpsert) {
                if(upcolumns != null && upcolumns.length > 0) {
                    Map<String, Object> updateRow = new HashMap<>();
                    for(String col : upcolumns) {
                        updateRow.put(col, row.get(col));
                    }
                    updateBuilder = transportClient.prepareUpdate(index, type, String.valueOf(row.get(idName))).setDoc(updateRow).setUpsert(row);
                } else {
                    updateBuilder = transportClient.prepareUpdate(index, type, String.valueOf(row.get(idName))).setDoc(row).setUpsert(row);
                }
            } else {
                indexBuilder = transportClient.prepareIndex(index, type, String.valueOf(row.get(idName))).setSource(row);
            }
            if(this.routingColumn != null) {
                routingValue = (String) row.get(routingColumn);
                if(routingValue != null) {
                    if(isUpsert) {
                        updateBuilder.setRouting(routingValue);
                    } else {
                        indexBuilder.setRouting(routingValue);
                    }
                }
            }
            if(this.parentColumn != null) {
                parentValue = (String) row.get(parentColumn);
                if(parentValue != null) {
                    if(isUpsert) {
                        updateBuilder.setParent(parentValue);
                    } else {
                        indexBuilder.setParent(parentValue);
                    }
                }
            }
            if(isUpsert) {
                bulkRequest.add(updateBuilder);
            } else {
                bulkRequest.add(indexBuilder);
            }
        }
        if((rowCount % batchSize == 0 || row.size() == 0) && rowCount > 0){
            LOGGER.info("本次提交索引{}条，已提交{}条.", rowCount, total);
            bulkResponse = bulkRequest.execute().actionGet();
            rowCount = 0;
            bulkRequest = transportClient.prepareBulk();
            //TODO: 记录提交错误的数据
            processRespItems(bulkResponse);
        }
    }

    protected void processRespItems(BulkResponse bulkResponse) {
        List<Map<String, Object>> errorResultList = new LinkedList<>();
        int succ = 0, fail = 0;
        for(BulkItemResponse itemResp : bulkResponse.getItems()) {
            if(itemResp.getFailure()!= null){
                fail++;
                LOGGER.error("ES数据保存失败:index={}, type={}, id={},失败信息:{}", itemResp.getIndex(), itemResp.getType(), itemResp.getId(),itemResp.getFailureMessage());
                //记录错误数据
                Map<String, Object> errorResult = new LinkedHashMap<>();
                errorResult.put("output_type", procOutputMeta.getProcOutputType().name());
                errorResult.put("index", itemResp.getIndex());
                errorResult.put("type", itemResp.getType());
                errorResult.put("id", itemResp.getId());
                errorResult.put("failMsg", itemResp.getFailureMessage());
                errorResultList.add(errorResult);
            } else {
                succ++;
            }
        }
        long t = stopWatch.getTime();
        LOGGER.info("成功索引{}条，失败{}条，共耗时{}ms,速度:{}条/秒", succ, fail, t, (int)((((double)total)*1000)/t));
/*
        LOGGER.info("任务{},成功索引{}条，失败{}条，共耗时{}", getKey(), succ,fail, stopWatch.getTime());
        if(errorResultList.size() > 0) {
            batchInsertErrors(SQL_COMMIT_ERROR, errorResultList);
        }
*/
    }

}
