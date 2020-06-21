package com.ropeok.dataprocess.v2.component.out;

import com.google.common.base.Preconditions;
import com.ropeok.dataprocess.common.JobRunException;
import com.ropeok.dataprocess.utils.Constants;
import com.ropeok.dataprocess.utils.DBUtils;
import com.ropeok.dataprocess.v2.component.AbstractComponent;
import com.ropeok.dataprocess.v2.core.IDataSet;
import com.ropeok.dataprocess.v2.core.SendEvent;
import oracle.jdbc.proxy.annotation.Pre;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class JDBCOutput extends AbstractComponent {

    private static final Logger LOGGER = LoggerFactory.getLogger(JDBCOutput.class);
    private Connection conn;
    private int batchSize;
    private PreparedStatement ps;
    private ISQLBuilder sqlBuilder;
    private static final int TRY_TIMES = 3;
    private int expTimes = 0;

    @Override
    public void init() throws Exception {
        String url = getStepMeta().getStringProperty(Constants.URL);
        String username = getStepMeta().getStringProperty(Constants.USER_NAME);
        String password = getStepMeta().getStringProperty(Constants.PASSWORD);
        batchSize = Integer.parseInt(getStepMeta().getOrDefaultStringProperty(Constants.BATCH_SIZE, "5000"));
        String mode = getStepMeta().getStringProperty(Constants.MODE);
        String table = getStepMeta().getStringProperty(Constants.TABLE);

        Preconditions.checkNotNull(table, "步骤[" + step.getName() + "]表名不能为空");

        String[] inColumns = getStepMeta().getStringPropertyToArray(Constants.IN_COLUMNS);
        String[] upColumns = getStepMeta().getStringPropertyToArray(Constants.UP_COLUMNS);
        String[] uniqueColumns = getStepMeta().getStringPropertyToArray(Constants.UNIQUE_COLUMNS);

        Map<String, Object> params = new HashMap<>();
        params.put("IN_COLUMNS", inColumns);
        params.put("UP_COLUMNS", upColumns);
        params.put("UNIQUE_COLUMNS", uniqueColumns);

        if(Constants.MODE_UPSERT.equals(mode)) {
            Preconditions.checkArgument(inColumns != null && inColumns.length > 0, "步骤["+step.getName()+"]未定义插入字段");
            Preconditions.checkArgument(upColumns != null && upColumns.length > 0, "步骤["+step.getName()+"]未定义更新字段");
            Preconditions.checkArgument(uniqueColumns != null && uniqueColumns.length > 0, "步骤["+step.getName()+"]未定义唯一性字段");

            if(url.contains(DBUtils.MYSQL)) {
                sqlBuilder = new MysqlSQLBuilder(mode);
                sqlBuilder.buildSQL(table, params);
                LOGGER.info("sql={}", sqlBuilder.getSql());
            } else if(url.contains(DBUtils.ORACLE)) {
                sqlBuilder = new OracleSQLBuilder(mode);
                sqlBuilder.buildSQL(table, params);
                LOGGER.info("sql={}", sqlBuilder.getSql());
            } else {
                throw new JobRunException("步骤[" + step.getName()+"]配置了不支持的模式");
            }
        } else if(Constants.MODE_INSERT.equals(mode)) {
            Preconditions.checkArgument(inColumns != null && inColumns.length > 0, "步骤["+step.getName()+"]未定义插入字段");
            sqlBuilder = new DefaultSQLBuilder(mode);
            sqlBuilder.buildSQL(table, params);
            LOGGER.info("sql={}", sqlBuilder.getSql());
        } else if(Constants.MODE_UPDATE.equals(mode)) {
            Preconditions.checkArgument(upColumns != null && upColumns.length > 0, "步骤["+step.getName()+"]未定义更新字段");
            sqlBuilder = new DefaultSQLBuilder(mode);
            sqlBuilder.buildSQL(table, params);
            LOGGER.info("sql={}", sqlBuilder.getSql());
        }
        this.conn = DBUtils.getConnection(url, username, password);
        this.conn.setAutoCommit(false);
        this.ps = conn.prepareStatement(sqlBuilder.getSql());
    }

    @Override
    public void exec(SendEvent event) throws Exception {
        if(event != null) {
            sqlBuilder.assignStatement(ps, event);
            if (stats.getCurrentRow() >= batchSize) {
                batchCommit();
            }
        }
        send(event);
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
        DBUtils.close(ps);
        DBUtils.close(conn);
        super.finished();
    }

    private void batchCommit() throws Exception {
        try {
            ps.executeBatch();
            conn.commit();
            ps.clearBatch();
//            ps.cancel();
            LOGGER.info("{}", stats.getCurrentInfo());
            stats.reset();
            expTimes = 0;
        } catch (BatchUpdateException batchEx) {
            batchEx.printStackTrace();
            expTimes++;
            if(expTimes <= TRY_TIMES) {
                LOGGER.warn("batch exception retry {}", expTimes);
                batchCommit();
            } else {
                throw batchEx;
            }
        }
    }

    public interface ISQLBuilder {
        public String buildSQL(String table, Map<String, Object> params);
        public void assignStatement(PreparedStatement ps, SendEvent event) throws SQLException;
        public String getSql();
    }

    public static abstract class AbstractSQLBuilder implements ISQLBuilder {

        protected StringBuilder sql = new StringBuilder();
        protected int index = 0;
        protected String[] inColumns;
        protected String[] upColumns;
        protected String[] uniqueColumns;
        protected String mode;

        public AbstractSQLBuilder(String mode) {
            Preconditions.checkNotNull(mode);
            this.mode = mode;
        }

        @Override
        public String buildSQL(String table, Map<String, Object> params) {
            if(Constants.MODE_UPSERT.equals(mode)) {
                return buildUpsert(table, params);
            } else if(Constants.MODE_UPDATE.equals(mode)) {
                return buildUpdate(table, params);
            } else {
                return buildInsert(table, params);
            }
        }

        protected abstract String buildUpsert(String table, Map<String, Object> params);
        protected abstract String buildUpdate(String table, Map<String, Object> params);
        protected abstract String buildInsert(String table, Map<String, Object> params);

        @Override
        public void assignStatement(PreparedStatement ps, SendEvent event) throws SQLException {
            if(Constants.MODE_UPSERT.equals(mode)) {
                assignStatementUpsert(ps, event);
            } else if(Constants.MODE_UPDATE.equals(mode)) {
                assignStatementUpdate(ps, event);
            } else {
                assignStatementInsert(ps, event);
            }
        }

        protected abstract void assignStatementUpsert(PreparedStatement ps, SendEvent event) throws SQLException;
        protected abstract void assignStatementUpdate(PreparedStatement ps, SendEvent event) throws SQLException;
        protected abstract void assignStatementInsert(PreparedStatement ps, SendEvent event) throws SQLException;

        @Override
        public String getSql() {
            return sql.toString();
        }
    }

    public static class DefaultSQLBuilder extends AbstractSQLBuilder {

        protected Collection<Map<String, Object>> datas;

        public DefaultSQLBuilder(String mode) {
            super(mode);
        }
        protected String buildInsert(String table, Map<String, Object> params) {
            sql.setLength(0);
            sql.append("INSERT INTO " + table).append(" (");
            this.inColumns = (String[]) params.get("IN_COLUMNS");
            int size = inColumns.length;
            StringBuilder inValues = new StringBuilder();
            for(int i = 0; i < size; i++) {
                sql.append(inColumns[i]);
                inValues.append("?");
                if(i < size-1) {
                    sql.append(",");
                    inValues.append(",");
                }
            }
            sql.append(") VALUES (").append(inValues.toString()).append(")");
            return sql.toString();
        }

        protected String buildUpdate(String table, Map<String, Object> params) {
            sql.setLength(0);
            sql.append("UPDATE " + table).append(" SET ");
            this.upColumns = (String[]) params.get("UP_COLUMNS");
            int size = upColumns.length;
            for (int i = 0; i < size; i++) {
                sql.append(upColumns[i]).append("=?");
                if(i < size-1) {
                    sql.append(",");
                }
            }
            sql.append(" WHERE ");
            this.uniqueColumns = (String[]) params.get("UNIQUE_COLUMNS");
            size = uniqueColumns.length;
            for(int i = 0; i < size; i++) {
                sql.append(uniqueColumns[i]).append("=?");
                if(i < size-1) {
                    sql.append(" AND ");
                }
            }
            return sql.toString();
        }

        @Override
        protected String buildUpsert(String table, Map<String, Object> params) {
            throw new UnsupportedOperationException("不支持此项操作");
        }

        protected void assignStatementInsert(PreparedStatement ps, SendEvent event) throws SQLException {
            this.datas = event.getDataSet().getData();
            for(Map<String, Object> data : datas) {
                this.index = 1;
                for(int i = 0, size = inColumns.length; i < size; i++) {
                    ps.setObject(index++, data.get(inColumns[i].trim()));
                }
                ps.addBatch();
            }
        }

        protected void assignStatementUpdate(PreparedStatement ps, SendEvent event) throws SQLException {
            this.datas = event.getDataSet().getData();
            for(Map<String, Object> data : datas) {
                this.index = 1;
                for(int i = 0, size = upColumns.length; i < size; i++) {
                    ps.setObject(index++, data.get(upColumns[i].trim()));
                }
                for(int i = 0, size = uniqueColumns.length; i < size; i++) {
                    ps.setObject(index++, data.get(uniqueColumns[i].trim()));
                }
                ps.addBatch();
            }
        }

        @Override
        protected void assignStatementUpsert(PreparedStatement ps, SendEvent event) throws SQLException {
            throw new UnsupportedOperationException("不支持此项操作");
        }
    }

    public static class MysqlSQLBuilder extends DefaultSQLBuilder {

        public MysqlSQLBuilder(String mode) {
            super(mode);
        }

        @Override
        protected String buildUpsert(String table, Map<String, Object> params) {
            sql.append("INSERT INTO " + table + " (");
            this.inColumns = (String[]) params.get("IN_COLUMNS");
            int size = inColumns.length;
            StringBuilder inValues = new StringBuilder();
            for(int i = 0; i < size; i++) {
                sql.append(inColumns[i]);
                inValues.append("?");
                if(i < size-1) {
                    sql.append(",");
                    inValues.append(",");
                }
            }
            sql.append(") VALUES (").append(inValues.toString()).append(") ON DUPLICATE KEY UPDATE ");
            this.upColumns = (String[]) params.get("UP_COLUMNS");
            size = upColumns.length;
            for (int i = 0; i < size; i++) {
                sql.append(upColumns[i]).append("=?");
                if(i < size-1) {
                    sql.append(",");
                }
            }
            return sql.toString();
        }

        @Override
        protected void assignStatementUpsert(PreparedStatement ps, SendEvent event) throws SQLException {
            this.datas = event.getDataSet().getData();
            for(Map<String, Object> data : datas) {
                this.index = 1;
                for(int i = 0, size = inColumns.length; i < size; i++) {
                    System.out.println(data.get(inColumns[i].trim()).getClass().toString() + " , "+ data.get(inColumns[i].trim()));
                    ps.setObject(index++, data.get(inColumns[i].trim()));
                }
                for(int i = 0, size = upColumns.length; i < size; i++) {
                    ps.setObject(index++, data.get(upColumns[i].trim()));
                }
                ps.addBatch();
            }
        }

    }

    public static class OracleSQLBuilder extends DefaultSQLBuilder {

        public OracleSQLBuilder(String mode) {
            super(mode);
        }

        @Override
        public String buildUpsert(String table, Map<String, Object> params) {
            sql.append("MERGE /*+ USE_MERGE(t1 t2) */ INTO " + table + " t1 ");
            sql.append("USING (SELECT ");
            this.inColumns = (String[]) params.get("IN_COLUMNS");
            int size = inColumns.length;
            for(int i = 0; i < size; i++) {
                sql.append("? AS " + inColumns[i]);
                if(i < (size -1)) {
                    sql.append(",");
                }
            }
            sql.append(" FROM dual) t2 ");
            sql.append("ON (");
            this.uniqueColumns = (String[]) params.get("UNIQUE_COLUMNS");
            size = uniqueColumns.length;
            for(int i = 0; i < size; i++) {
                sql.append("t1." + uniqueColumns[i] + "=t2." + uniqueColumns[i]);
            }
            sql.append(") ");
            sql.append("WHEN NOT MATCHED THEN ");
            sql.append(" INSERT (");
            size = inColumns.length;
            StringBuilder inValues = new StringBuilder();
            for(int i = 0; i < size; i++) {
                sql.append(inColumns[i]);
                inValues.append("?");
                if(i < size-1) {
                    sql.append(",");
                    inValues.append(",");
                }
            }
            sql.append(") VALUES (").append(inValues.toString()).append(") ");
            sql.append(" WHEN MATCHED THEN ");
            sql.append(" UPDATE SET ");
            this.upColumns = (String[]) params.get("UP_COLUMNS");
            size = upColumns.length;
            for(int i = 0; i < size; i++) {
                sql.append(upColumns[i] + "=?");
                if(i < size-1) {
                    sql.append(",");
                }
            }
            return sql.toString();
        }

        @Override
        public void assignStatementUpsert(PreparedStatement ps, SendEvent event) throws SQLException {
            this.datas = event.getDataSet().getData();
            for(Map<String, Object> data : datas) {
                this.index = 1;
                for(int i = 0, size = inColumns.length; i < size; i++) {
                    ps.setObject(index++, data.get(inColumns[i].trim()));
                }
                for(int i = 0, size = inColumns.length; i < size; i++) {
//                    System.out.println(data.get(inColumns[i].trim()).getClass().toString() + " , "+ data.get(inColumns[i].trim()));
                    ps.setObject(index++, data.get(inColumns[i].trim()));
                }
                for(int i = 0, size = upColumns.length; i < size; i++) {
                    ps.setObject(index++, data.get(upColumns[i].trim()));
                }
                ps.addBatch();
            }
        }
    }

}
