package com.github.yafeiwang1240.sparkoperator.output;

import com.github.yafeiwang1240.Function;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * output to redis
 * @author wangyafei
 */
public class ESSink implements Function {

    @Override
    public void function() {

        try(SparkSession session = SparkSession.builder().appName("redisSink")
                .enableHiveSupport().getOrCreate()) {
            org.apache.spark.sql.Dataset<Row> ds = session.sql("select c_name as name, c_workyear as value from user_c limit 100");
            ds.foreachPartition(new ForeachPartitionFunction<Row>() {
                @Override
                public void call(Iterator<Row> t) throws Exception {
                    try (TransportClient client = new PreBuiltTransportClient(Settings.EMPTY)
                            .addTransportAddress(new TransportAddress(InetAddress.getByName("127.0.0.1"), 9300));){
                        while (t.hasNext()) {
                            Row row = t.next();
                            client.index(new IndexRequest("index", "_doc")
                                    .source(rowToMap(row))).actionGet();
                        }
                    }
                }
            });
        }
    }


    private static Map<String, Object> rowToMap(Row row) {
        Map<String, Object> map = new HashMap<>(row.size());
        String[] keys = row.schema().fieldNames();
        for (int i = 0; i < row.size(); i++) {
            Object value = row.get(i);
            if (value != null) {
                map.put(keys[i], value);
            }
        }
        return map;
    }
    public static void main(String[] args) throws UnknownHostException {
        TransportClient client = new PreBuiltTransportClient(Settings.EMPTY)
                .addTransportAddress(new TransportAddress(InetAddress.getByName("127.0.0.1"), 9300));
        client.close();
    }
}
