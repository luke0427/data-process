package com.ropeok.dataprocess.inout.input;

import com.google.common.base.Preconditions;
import com.ropeok.dataprocess.meta.IncreMeta;
import com.ropeok.dataprocess.utils.Constants;
import com.ropeok.dataprocess.utils.DBUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.Map;

public class JDBCProcInput extends AbstractProcInput {

    private static final Logger LOGGER = LoggerFactory.getLogger(JDBCProcInput.class);
    private Connection conn;
    private PreparedStatement ps;
    private ResultSet rs;
    private ResultSetMetaData rsMeta;
    private int colSize;
    private static final DateTimeFormatter YYYY_MM_DD_HH_MM_SS = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    @Override
    public void init() throws Exception {
        Preconditions.checkNotNull(procInputMeta);
        Preconditions.checkNotNull(procInputMeta.getUrl());
        super.preInit();
        IncreMeta increMeta = procInputMeta.getIncreMeta();
        String imStartTime = increMeta != null ? increMeta.getStartTime() : null;
        String imEndTime = increMeta != null ? increMeta.getEndTime() : null;
        if(increMeta != null) {
            String sql = procInputMeta.getSql();
            //TODO : 判断sql是否有where条件如果有就需要，可能没有where但是带有其他查询关键字，所以只能替换表名后面和关键字中间，这里先简单处理后面需要判断
            sql += " WHERE ";
            if(this.endTime != null && StringUtils.isNotBlank(imStartTime) && ((imStartTime.contains(Constants.PROC_LAST_DATE)) || imStartTime.contains(Constants.PROC_LAST_TIME))) {
//                LocalDateTime now = LocalDateTime.now();
//                LocalDateTime pdt  = this.endTime;
                this.startTime = this.endTime;
                sql += (increMeta.getColumn() + " >= ? ");//AND ";
            } else if(StringUtils.isNotBlank(imStartTime) && (imStartTime.contains(Constants.PROC_PARAM_DATE) || imStartTime.contains(Constants.PROC_PARAM_TIME))) {
                this.startTime = LocalDateTime.parse(jobDetail.getJobDataMap().getString("START_TIME"), YYYY_MM_DD_HH_MM_SS);
                sql += (increMeta.getColumn() + " >= ? ");
            }

            if(StringUtils.isNotBlank(imEndTime) && (imEndTime.contains(Constants.PROC_CURRENT_DATE) || imEndTime.contains(Constants.PROC_CURRENT_TIME) || imEndTime.contains(Constants.PROC_PARAM_DATE) || imEndTime.contains(Constants.PROC_PARAM_TIME))) {
                if(imEndTime.contains(Constants.PROC_PARAM_DATE) || imEndTime.contains(Constants.PROC_PARAM_TIME)) {
                    this.endTime = LocalDateTime.parse(jobDetail.getJobDataMap().getString(Constants.END_TIME), YYYY_MM_DD_HH_MM_SS);
                } else {
                    this.endTime = LocalDateTime.now();
                }
                if(this.startTime != null) {
                    sql += " AND ";
                }
                sql +=  (increMeta.getColumn() + " < ? ");
            }
            procInputMeta.setSql(sql);
        }

        this.conn = DBUtils.getConnection(procInputMeta.getUrl(), procInputMeta.getUsername(), procInputMeta.getPassword());
        Preconditions.checkNotNull(conn);
        String sql = procInputMeta.getSql();
        LOGGER.info("sql={}", sql);
        this.ps = this.conn.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);

        if(procInputMeta.getUrl().contains("mysql")) {
            ps.setFetchSize(Integer.MIN_VALUE);
        } else {
           ps.setFetchSize(procInputMeta.getFetchSize());
        }
        if(this.startTime != null) {
            ZonedDateTime zonedDateTime = this.startTime.atZone(ZoneId.systemDefault());
            this.sd = java.util.Date.from(zonedDateTime.toInstant());
            if(StringUtils.isNotBlank(imStartTime) && (imStartTime.contains(Constants.PROC_LAST_DATE) || imStartTime.contains(Constants.PROC_PARAM_DATE))) {
                LOGGER.info("ZoneId={}, StartDate={}", ZoneId.systemDefault(), new java.sql.Date(this.sd.getTime()));
                ps.setDate(1, new java.sql.Date(this.sd.getTime()));
            } else {
                LOGGER.info("ZoneId={}, StartDate={}", ZoneId.systemDefault(), new Timestamp(this.sd.getTime()));
                ps.setTimestamp(1, new Timestamp(this.sd.getTime()));
            }
        }
        if(this.endTime != null) {
            ZonedDateTime zonedDateTime = this.endTime.atZone(ZoneId.systemDefault());
            this.ed = java.util.Date.from(zonedDateTime.toInstant());
            if(StringUtils.isNotBlank(imEndTime) && (imEndTime.contains(Constants.PROC_CURRENT_DATE) || imEndTime.contains(Constants.PROC_PARAM_DATE))) {
                LOGGER.info("ZoneId={}, EndDate={}", ZoneId.systemDefault(), new java.sql.Date(this.ed.getTime()));
                ps.setDate(this.startTime == null ? 1 : 2, new java.sql.Date(this.ed.getTime()));
            } else {
                LOGGER.info("ZoneId={}, EndDate={}", ZoneId.systemDefault(), new Timestamp(this.ed.getTime()));
                ps.setTimestamp(this.startTime == null ? 1 : 2, new Timestamp(this.ed.getTime()));
            }
        }

        rs = ps.executeQuery();
        this.rsMeta = rs.getMetaData();
        this.colSize = rsMeta.getColumnCount();
    }

    @Override
    public boolean hasNext() throws Exception{
        return rs.next();
    }

    @Override
    public Map<String, Object> next() throws Exception{
        Map<String, Object> row = new LinkedHashMap<String, Object>();
        for (int i = 0; i < this.colSize; ++i) {
            String columName = rsMeta.getColumnLabel(i + 1);
            Object value = rs.getObject(i + 1);
            row.put(columName, value);
        }
        return row;
    }

    @Override
    public void finished() throws Exception {
        LOGGER.info("执行结束工作");
        DBUtils.close(rs);
        DBUtils.close(ps);
        DBUtils.close(conn);
    }

    @Override
    public void onError() throws Exception {
    }

}
