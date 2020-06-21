package com.ropeok.dataprocess.handler.impl;

import com.ropeok.dataprocess.handler.HandleStatus;
import com.ropeok.dataprocess.meta.ColumnMeta;
import com.ropeok.dataprocess.utils.DBUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;

public class JDBCCacheProcHandler extends AbstractProcHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(JDBCCacheProcHandler.class);

    protected final Map<String, Object> CACHE_MAP = new HashMap<>();

    @Override
    public void init() {
        super.init();
        LOGGER.info("开始初始化缓存");
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            conn = DBUtils.getConnection(procHandlerMeta.getUrl(), procHandlerMeta.getUsername(), procHandlerMeta.getPassword());
            ps = conn.prepareStatement(procHandlerMeta.getSql(), ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            rs = ps.executeQuery();
            ResultSetMetaData rsMeta = rs.getMetaData();
            int size = rsMeta.getColumnCount();
            while(rs.next()) {
                Map<String, Object> row = new LinkedHashMap<String, Object>();
                for (int i = 0; i < size; ++i) {
                    String columName = rsMeta.getColumnLabel(i + 1);
                    Object value = rs.getObject(i + 1);
                    row.put(columName, value);
                }
                if("true".equals(procHandlerMeta.getMultiValue())) {
                    Set<String> cacheValues = (Set<String>) CACHE_MAP.get((String) row.get(procHandlerMeta.getCacheKeyCol()));
                    if(cacheValues == null) {
                        cacheValues = new LinkedHashSet<String>();
                        CACHE_MAP.put((String) row.get(procHandlerMeta.getCacheKeyCol()), cacheValues);
                    }
                    List<ColumnMeta> columnMetas = procHandlerMeta.getColumnMetas();
                    if(columnMetas.size() >= 1) {
                        //多值字段只支持缓存一个字段
                        Object value = row.get(columnMetas.get(0).getColumnName());
                        if(value != null) {
                            cacheValues.add(String.valueOf(value));
                        }
                    }
                } else {
                    CACHE_MAP.put((String) row.get(procHandlerMeta.getCacheKeyCol()), row);
                }
            }
            LOGGER.info("JDBC缓存初始化完成共缓存{}条记录", CACHE_MAP.size());
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException(e.getLocalizedMessage());
        } finally {
            DBUtils.close(rs);
            DBUtils.close(ps);
            DBUtils.close(conn);
        }
    }

    @Override
    public HandleStatus handle(Map<String, Object> data) throws Exception {
        if("true".equals(procHandlerMeta.getMultiValue())) {
            Set<Map<String, Object>> cacheRow = (Set<Map<String, Object>>) CACHE_MAP.get(data.get(procHandlerMeta.getKeyColumn()));
            if(cacheRow != null) {
                Iterator<ColumnMeta> it = procHandlerMeta.getColumnMetas().iterator();
                while(it.hasNext()) {
                    ColumnMeta columnMeta = it.next();
                    data.put(columnMeta.getToName(), cacheRow);
                }
            }
        } else {
            Map<String, Object> cacheRow = (Map<String, Object>) CACHE_MAP.get(data.get(procHandlerMeta.getKeyColumn()));
            if(cacheRow != null) {
                Iterator<ColumnMeta> it = procHandlerMeta.getColumnMetas().iterator();
                while(it.hasNext()) {
                    ColumnMeta columnMeta = it.next();
                    data.put(columnMeta.getToName(), cacheRow.get(columnMeta.getColumnName()));
                }
            }
        }
        if(nextHandler != null) {
            return nextHandler.handle(data);
        }
        return HandleStatus.SUCC;
    }
}
