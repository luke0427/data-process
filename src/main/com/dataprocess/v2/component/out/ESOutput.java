package com.ropeok.dataprocess.v2.component.out;

import com.ropeok.dataprocess.utils.Constants;
import com.ropeok.dataprocess.v2.component.AbstractComponent;
import com.ropeok.dataprocess.v2.core.SendEvent;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
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
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class ESOutput extends AbstractComponent {

    private static final Logger LOGGER = LoggerFactory.getLogger(ESOutput.class);
    private TransportClient transportClient;
    private int batchSize;
    private boolean isUpsert;
    private String[] upColumns;
    private String index;
    private String idxType;
    private String idColumn;
    private String routingColumn;
    private String routingValue;
    private String parentColumn;
    private String parentValue;
    private String delColumn;
    private String delValue;
    private String value;
    private Collection<Map<String, Object>> datas;
    private IndexRequestBuilder indexBuilder;
    private UpdateRequestBuilder updateBuilder;
    private DeleteRequestBuilder deleteBuilder;
    private BulkRequestBuilder bulkRequest;
    private BulkResponse bulkResponse;

    @Override
    public void init() throws Exception {
        String clusterName = getStepMeta().getStringProperty(Constants.CLUSTER_NAME);
        Settings.Builder settingsBuilder = Settings.builder().put("cluster.name", clusterName);
        String host = getStepMeta().getStringProperty(Constants.HOST);
        String username = getStepMeta().getStringProperty(Constants.USER_NAME);
        if(StringUtils.isNotBlank(username)) {
            settingsBuilder.put("xpack.security.user", username + ":" + getStepMeta().getStringProperty(Constants.PASSWORD));
        }
        int port = getStepMeta().getIntProperty(Constants.PORT);
        LOGGER.info("host:{}, port:{}, name:{}", host, port, clusterName);
        Settings settings = settingsBuilder.build();
        try {
            if(!StringUtils.isBlank(username)) {
//                transportClient = new PreBuiltXPackTransportClient(settings)
//                        .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host), port));
                transportClient = new PreBuiltXPackTransportClient(settings)
                        .addTransportAddress(new TransportAddress(InetAddress.getByName(host), port));
            } else {
//                transportClient = new PreBuiltTransportClient(settings).addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host), port));
                transportClient = new PreBuiltTransportClient(settings).addTransportAddress(new TransportAddress(InetAddress.getByName(host), port));
            }
        } catch (UnknownHostException e) {
            LOGGER.error("ES输出客户端创建失败");
        }
        batchSize = Integer.parseInt(getStepMeta().getOrDefaultStringProperty(Constants.BATCH_SIZE, "5000"));
        index = getStepMeta().getStringProperty(Constants.INDEX);
        idxType = getStepMeta().getStringProperty(Constants.IDX_TYPE);
        idColumn = getStepMeta().getStringProperty(Constants.ID_COLUMN);
        routingColumn = getStepMeta().getStringProperty(Constants.ROUTING_COLUMN);
        parentColumn = getStepMeta().getStringProperty(Constants.PARENT_COLUMN);
        String mode = getStepMeta().getStringProperty(Constants.MODE);
        isUpsert = Constants.MODE_UPSERT.equals(mode);
        upColumns = getStepMeta().getStringPropertyToArray(Constants.UP_COLUMNS);
        this.delColumn = getStepMeta().getStringProperty(Constants.DEL_COLUMN);
        this.delValue = getStepMeta().getStringProperty(Constants.DEL_VALUE);
        bulkRequest = transportClient.prepareBulk();
    }

    @Override
    public void exec(SendEvent event) throws Exception {
        if(event != null) {
            datas = event.getDataSet().getData();
            for(Map<String, Object> data : datas) {
                buildData(data);
            }
            if (stats.getCurrentRow() >= batchSize) {
                batchCommit();
            }
        }
        send(event);
    }

    private void buildData(Map<String, Object> data) {
        if(delColumn != null) {
            value = (String) data.get(delColumn);
            if(delValue.equals(value)) {
                deleteBuilder = transportClient.prepareDelete(index, idxType, String.valueOf(data.get(idColumn)));
                bulkRequest.add(deleteBuilder);
                return;
            }
        }
        if (isUpsert) {
            if(upColumns != null && upColumns.length > 0) {
                Map<String, Object> updateRow = new HashMap<>();
                for(String col : upColumns) {
                    updateRow.put(col, data.get(col));
                }
                updateBuilder = transportClient.prepareUpdate(index, idxType, String.valueOf(data.get(idColumn))).setDoc(updateRow).setUpsert(data);
            } else {
                updateBuilder = transportClient.prepareUpdate(index, idxType, String.valueOf(data.get(idColumn))).setDoc(data).setUpsert(data);
            }
        } else {
            indexBuilder = transportClient.prepareIndex(index, idxType, String.valueOf(data.get(idColumn))).setSource(data);
        }
        if(this.routingColumn != null) {
            routingValue = (String) data.get(routingColumn);
            if(routingValue != null) {
                if(isUpsert) {
                    updateBuilder.setRouting(routingValue);
                } else {
                    indexBuilder.setRouting(routingValue);
                }
            }
        }
        if(this.parentColumn != null) {
            parentValue = (String) data.get(parentColumn);
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

    @Override
    protected void beforeFinished() throws Exception {
        super.beforeFinished();
        if(stats.getCurrentRow() > 0) {
            batchCommit();
        }
    }

    @Override
    public void finished() throws Exception {
        this.transportClient.close();
        super.finished();
    }

    private void batchCommit() {
        bulkResponse = bulkRequest.execute().actionGet();
        bulkRequest = transportClient.prepareBulk();
        processRespItems(bulkResponse);
        LOGGER.info("{}", stats.getCurrentInfo());
        stats.reset();
    }

    protected void processRespItems(BulkResponse bulkResponse) {
//        List<Map<String, Object>> errorResultList = new LinkedList<>();
        int succ = 0, fail = 0;
        for(BulkItemResponse itemResp : bulkResponse.getItems()) {
            if(itemResp.getFailure()!= null){
                fail++;
                LOGGER.error("ES数据保存失败:index={}, type={}, id={},失败信息:{}", itemResp.getIndex(), itemResp.getType(), itemResp.getId(),itemResp.getFailureMessage());
                //记录错误数据
                /*Map<String, Object> errorResult = new LinkedHashMap<>();
                errorResult.put("output_type", procOutputMeta.getProcOutputType().name());
                errorResult.put("index", itemResp.getIndex());
                errorResult.put("type", itemResp.getType());
                errorResult.put("id", itemResp.getId());
                errorResult.put("failMsg", itemResp.getFailureMessage());
                errorResultList.add(errorResult);*/
            } else {
                succ++;
            }
        }
        LOGGER.info("成功索引{}条，失败{}条", succ, fail);
    }
}
