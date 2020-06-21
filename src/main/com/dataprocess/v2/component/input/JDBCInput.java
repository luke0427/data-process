package com.ropeok.dataprocess.v2.component.input;

import com.google.common.base.Preconditions;
import com.ropeok.dataprocess.utils.Constants;
import com.ropeok.dataprocess.utils.DBUtils;
import com.ropeok.dataprocess.v2.component.AbstractComponent;
import com.ropeok.dataprocess.v2.core.IDataSet;
import com.ropeok.dataprocess.v2.core.SendEvent;
import com.ropeok.dataprocess.v2.core.impl.DataSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.Reader;
import java.sql.*;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;

public class JDBCInput extends AbstractComponent{

    private static final Logger LOGGER = LoggerFactory.getLogger(JDBCInput.class);
    private Connection conn;
    private ResultSet rs;
    private PreparedStatement ps;
    private ResultSetMetaData rsMeta;
    private int columnSize;
    private boolean incre;
    private String increColumn;

    @Override
    public void init() throws Exception {
        String url = getStepMeta().getStringProperty(Constants.URL);
        String username = getStepMeta().getStringProperty(Constants.USER_NAME);
        String password = getStepMeta().getStringProperty(Constants.PASSWORD);
        String fetchSize = getStepMeta().getOrDefaultStringProperty(Constants.FETCH_SIZE, "5000");
        String sql = getStepMeta().getStringProperty(Constants.STEP_BODY);
        this.incre = getStepMeta().getBooleanProperty(Constants.INCRE);
        Object stepEndTime = null;
        if(incre) {
            /*Map<String, Object> result = step.getContext().getJdbcTemplate().queryForMap("SELECT STEP_END_TIME FROM " + Constants.TABLE_STEP_INFO + " WHERE JOB_GROUP=? AND JOB_NAME=? AND JOB_STEP=?", step.getContext().getGroup(), step.getContext().getName(), step.getId());*/
            Map<String, Object> result = step.getContext().getJdbcTemplate().queryForMap("SELECT\n" +
                    "	CASE t.JOB_EXEC_RESULT\n" +
                    "WHEN 0 THEN\n" +
                    "	t.JOB_END_TIME\n" +
                    "WHEN 1 THEN\n" +
                    "	(\n" +
                    "		SELECT\n" +
                    "			k.STEP_END_TIME\n" +
                    "		FROM\n" +
                    Constants.TABLE_STEP_INFO +
                    "		 k \n" +
                    "		WHERE\n" +
                    "			k.JOB_GROUP = t.JOB_GROUP\n" +
                    "		AND k.JOB_NAME = t.JOB_NAME\n" +
                    "		AND k.JOB_STEP = ?\n" +
                    "	)\n" +
                    "END AS STEP_END_TIME\n" +
                    "FROM\n" +
                    Constants.TABLE_JOB_INFO +
                    "	 t \n" +
                    "WHERE\n" +
                    "	t.JOB_GROUP = ?\n" +
                    "AND t.JOB_NAME = ?", step.getId(), step.getContext().getGroup(), step.getContext().getName());
            stepEndTime = result.get("STEP_END_TIME");
            increColumn = getStepMeta().getStringProperty(Constants.INCRE_COLUMN);
            Preconditions.checkNotNull(increColumn, "增量字段未配置");
            if(stepEndTime != null) {
                sql += " WHERE " + increColumn + " >= ?";
                //如果是LAST_TIME或者PARAM_TIME就按照时间进行抽取，如果是LAST_DATE或者PARAM_DATE就按照日期进行抽取
            }
        }
        LOGGER.info("sql={}", sql);
        conn = DBUtils.getConnection(url, username, password);
        this.ps = conn.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        if(incre && stepEndTime != null) {
            String increStartTime = getStepMeta().getStringProperty(Constants.INCRE_START_TIME);
            if(increStartTime != null && (increStartTime.contains(Constants.PROC_LAST_TIME) || increStartTime.contains(Constants.PROC_PARAM_TIME))) {
                LOGGER.info("增量抽取时间(TIME):{}", stepEndTime);
                ps.setTimestamp(1, (Timestamp) stepEndTime);
            } else if(increStartTime != null && (increStartTime.contains(Constants.PROC_LAST_DATE) || increStartTime.contains(Constants.PROC_PARAM_DATE))) {
                java.sql.Date startDate = new java.sql.Date(((Timestamp)stepEndTime).getTime());
                LOGGER.info("增量抽取时间(DATE):{}", startDate);
                ps.setDate(1, startDate);
            }
        }
        if(url.contains(DBUtils.MYSQL)) {
            ps.setFetchSize(Integer.MIN_VALUE);
        } else {
            ps.setFetchSize(Integer.parseInt(fetchSize));
        }
    }

    @Override
    public void exec(SendEvent event) throws Exception {
        if(event == null) {
            rs = ps.executeQuery();
            this.rsMeta = rs.getMetaData();
            this.columnSize = rsMeta.getColumnCount();
            while(rs.next()) {
                IDataSet dataSet = new DataSet();
                Map<String, Object> data = next();
                dataSet.addData(data);
                if(this.incre) {
                    compareIncreValue(data.get(this.increColumn));
                }
                send(new SendEvent(dataSet));
                stats.increRow();
            }
        } else {
            //TODO：后面再考虑当有上一步数据传入的时候要做什么事
            send(event);
        }
    }

    private void compareIncreValue(Object value) {
        if(maxUpdateDate != null) {
//            System.out.println("maxUpdateDate:" + maxUpdateDate + ", rowDate:" + value);
            maxUpdateDate = maxUpdateDate.getTime() > ((Date) value).getTime() ? maxUpdateDate : (Date) value;
        } else {
            maxUpdateDate = (Date) value;
        }
    }

    private Map<String, Object> next() throws Exception{
        Map<String, Object> row = new LinkedHashMap<String, Object>();
        Reader is;
        BufferedReader br;
        String s, columnName;
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < this.columnSize; ++i) {
            columnName = rsMeta.getColumnLabel(i + 1);
            Object value = rs.getObject(i + 1);
            if(value instanceof Blob) {
                Blob blob = (Blob) value;
                InputStream in = blob.getBinaryStream();
                byte[] bytes = new byte[(int) blob.length()];
                in.read(bytes);
                in.close();
                value = bytes;
            } else if(value instanceof Clob) {
                Clob clob = (Clob) value;
                is = clob.getCharacterStream();// 得到流
                br = new BufferedReader(is);
                s = br.readLine();
                while (s != null) {
                    sb.append(s);
                    s = br.readLine();
                }
                value = sb.toString();
                sb.setLength(0);
                br.close();
                is.close();
            }
            row.put(columnName, value);
        }
        return row;
    }

    @Override
    public void finished() throws Exception {
        DBUtils.close(rs);
        DBUtils.close(ps);
        DBUtils.close(conn);
        super.finished();
    }

}
