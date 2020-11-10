package com.github.yafeiwang1240.sparkoperator.thrift;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.Closeable;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class SparkConnection implements Closeable {

	protected static final Log LOG = LogFactory.getLog(SparkConnection.class);

	protected Connection conn;
	/**
	 * 创建 Hive Connection.
	 *
	 * @param conn
	 */
	SparkConnection(Connection conn) {
		this.conn = conn;
	}

	public ExplainResult explain(String sql) throws SQLException {
        ExplainResult result = ExplainResult.newSuccessResult();
	    if (!StringUtils.startsWith(sql, "explain")) {
	        sql = "explain " + sql;
        }
	    try (Statement statement = conn.createStatement();
             ResultSet set = statement.executeQuery(sql)) {
	        set.next();
            String plan = set.getString("plan");
            if (StringUtils.contains(plan, "AnalysisException")) {
                result.setRes(false);
            }
            result.setMessage(plan);
            return result;
        }
    }


    @Override
    public void close() throws IOException {
        try {
            conn.close();
        } catch (SQLException e) {
            LOG.warn(e.getMessage(), e);
            throw new IOException(e);
        }
    }
}
