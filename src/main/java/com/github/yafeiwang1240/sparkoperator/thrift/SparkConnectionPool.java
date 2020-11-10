package com.github.yafeiwang1240.sparkoperator.thrift;

import org.apache.commons.dbcp.ConnectionFactory;
import org.apache.commons.dbcp.DriverConnectionFactory;
import org.apache.commons.dbcp.PoolableConnectionFactory;
import org.apache.commons.dbcp.PoolingDataSource;
import org.apache.commons.pool.impl.GenericObjectPool;

import java.io.Closeable;
import java.io.IOException;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/**
 * 使用spark thrift server检查sql
 * @author wangyafei
 */
public class SparkConnectionPool implements Closeable {

    private static final String DRIVER = "org.apache.hive.jdbc.HiveDriver";

    private static final SparkConnectionPool instance = new SparkConnectionPool();

    private static final String TEST_HQL = "explain select * from user_c limit 10";

    private GenericObjectPool<SparkConnection> objectPool;

    private PoolingDataSource poolingDataSource;

    private SparkConnectionPool() {
        try {
            Class.forName(DRIVER);
            Properties props = new Properties();
            props.setProperty("user", "hadoop");
            props.setProperty("password", "");
            String url = "jdbc:hive2://10.110.14.194:10016,10.110.13.58:10016";
            objectPool = new GenericObjectPool<>();
            objectPool.setTestWhileIdle(true);
            objectPool.setMaxActive(50);
            Driver driver = DriverManager.getDriver(url);
            ConnectionFactory connectionFactory = new DriverConnectionFactory(driver, url, props);
            PoolableConnectionFactory poolConnectionFactory = new PoolableConnectionFactory(
                    connectionFactory, objectPool, null, null,
                    false, true);
            poolConnectionFactory.setValidationQuery(TEST_HQL);
            poolingDataSource = new PoolingDataSource(objectPool);
            for (int i = 0; i < 2; i++) {
                objectPool.addObject();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static SparkConnectionPool getInstance() {
        return instance;
    }

    public synchronized SparkConnection getConnection() throws SQLException {
        Connection connection = poolingDataSource.getConnection();
        return new SparkConnection(connection);
    }

    @Override
    public void close() throws IOException {
        try {
            objectPool.close();
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    public static void main(String[] args) throws SQLException, IOException {
        SparkConnection connection = getInstance().getConnection();
        ExplainResult result = connection.explain("explain select c_name as name from user_c");
        System.out.println(result);
        connection.close();
        getInstance().close();
    }
}
