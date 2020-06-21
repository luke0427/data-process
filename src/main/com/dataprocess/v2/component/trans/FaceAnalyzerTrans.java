package com.ropeok.dataprocess.v2.component.trans;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.ropeok.dataprocess.utils.Constants;
import com.ropeok.dataprocess.v2.component.AbstractComponent;
import com.ropeok.dataprocess.v2.core.SendEvent;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.Collection;
import java.util.Map;

/**
 * 人脸分析
 */
public class FaceAnalyzerTrans extends AbstractComponent{

    private static final Logger LOGGER = LoggerFactory.getLogger(FaceAnalyzerTrans.class);

    private PoolingHttpClientConnectionManager cm;
    private RequestConfig requestConfig;
    private CloseableHttpClient httpClient;
    private HttpEntity postEntity;
    private HttpPost ksHttpPost;
    private HttpPost stHttpPost;
    private CloseableHttpResponse response;
    private String faceColumn;
    private boolean isRemoveFile;
    private Collection<Map<String, Object>> datas;
    private String respContent;
    private JSONObject result;
    private JSONArray faces;
    private JSONObject face;
    private JSONObject faceAttrs;
    private JSONObject facePose;
    private JSONObject faceRect;
    private JSONArray qualityDatas;

    @Override
    public void init() throws Exception {
        cm = new PoolingHttpClientConnectionManager();
        cm.setMaxTotal(10); // 最大连接数
        cm.setDefaultMaxPerRoute(cm.getMaxTotal());
        requestConfig = RequestConfig.custom()
                .setConnectTimeout(10 * 1000)    // 请求超时时间
                .setSocketTimeout(60 * 1000)    // 等待数据超时时间
                .setConnectionRequestTimeout(5000)  // 连接超时时间
                .build();
        ksHttpPost = new HttpPost(getStepMeta().getStringProperty(Constants.KS_URL) + "/detect");
        String stUrl = getStepMeta().getStringProperty(Constants.ST_URL);
        if(StringUtils.isNotBlank(stUrl)) {
            stHttpPost = new HttpPost(stUrl + "/verify/face/detectAndQuality");
        }
        this.faceColumn = getStepMeta().getStringProperty(Constants.COLUMN);
        this.isRemoveFile = getStepMeta().getBooleanProperty(Constants.REMOVE_FILE);
    }

    private CloseableHttpClient getHttpClient() {
        httpClient = HttpClients.custom().setConnectionManager(cm).setDefaultRequestConfig(requestConfig).build();
        return httpClient;
    }

    @Override
    public void exec(SendEvent event) throws Exception {
        if (event != null) {
            datas = event.getDataSet().getData();
            for (Map<String, Object> data : datas) {
                if(data.get(faceColumn) != null) {
                    ksFaceAnalyze(data);
                }
                if(isRemoveFile) {
                    data.remove(faceColumn);
                }
            }
        }
        send(event);
    }

    private void ksFaceAnalyze(Map<String, Object> data) throws IOException {
        ksHttpPost.reset();
        postEntity = MultipartEntityBuilder.create().addTextBody("analyze", "true").addTextBody("crop", "false").addBinaryBody("image", (byte[]) data.get(faceColumn), ContentType.MULTIPART_FORM_DATA, "face.jpg").build();
        ksHttpPost.setEntity(postEntity);
        try {
            response = getHttpClient().execute(ksHttpPost);
            if (response.getStatusLine().getStatusCode() == 200) {
                respContent = EntityUtils.toString(response.getEntity());
                if(respContent != null) {
                    result = JSONObject.parseObject(respContent);
                    if(result != null && result.containsKey("faces")) {
                        faces = result.getJSONArray("faces");
                        if(faces != null && !faces.isEmpty()) {
                            face = faces.getJSONObject(0);
                            data.put("confidence", face.getString("confidence"));

                            faceAttrs = face.getJSONObject("attrs");
                            data.put("age", faceAttrs.getString("age"));

                            data.put("blurness", faceAttrs.getJSONObject("face_quality").getString("blurness"));

                            faceRect = face.getJSONObject("rect");
                            data.put("width", faceRect.getString("width"));
                            data.put("height", faceRect.getString("height"));

                            facePose = faceAttrs.getJSONObject("pose");
                            data.put("yaw", facePose.getString("yaw"));
                            data.put("roll", facePose.getString("roll"));

                            //调用商汤获取图片质量
                            if (stHttpPost != null) {
                                stFaceAnalyze(data);
                            }
                        }
                    }
                }
            } else {
                LOGGER.error("ks返回状态码:{}, data={}", response.getStatusLine().getStatusCode(), data);
            }
        } finally {
            if (response != null) {
                response.close();
            }
        }

    }

    private void stFaceAnalyze(Map<String, Object> data) throws IOException {
        stHttpPost.reset();
        postEntity = MultipartEntityBuilder.create().addBinaryBody("imageData", (byte[]) data.get(faceColumn), ContentType.APPLICATION_OCTET_STREAM, "face.jpg").build();
        stHttpPost.setEntity(postEntity);
        int times = 0;
        while (times++ < 3) {
            try {
                response = getHttpClient().execute(stHttpPost);
                if(response.getStatusLine().getStatusCode() == 200) {
                    respContent = EntityUtils.toString(response.getEntity());
                    if(respContent != null) {
                        result = JSONObject.parseObject(respContent);
                        if("success".equals(result.getString("result"))) {
                            qualityDatas = result.getJSONArray("data");
                            if(qualityDatas != null && !qualityDatas.isEmpty()) {
                                data.put("quality_score", String.valueOf(qualityDatas.getJSONObject(0).getDoubleValue("quality_score")/100));
                            }
                        }
                    }
                } else {
                    LOGGER.error("st返回状态码:{}, data={}", response.getStatusLine().getStatusCode(), data);
                }
                break;
            } catch (SocketTimeoutException ste) {
                LOGGER.warn("st socket timeout retry {},", times);
                if(times >= 2) {
                    throw ste;
                }
            }
        }
    }

    @Override
    public void finished() throws Exception {
        if(httpClient != null) {
            httpClient.close();
        }
        super.finished();
    }
}
