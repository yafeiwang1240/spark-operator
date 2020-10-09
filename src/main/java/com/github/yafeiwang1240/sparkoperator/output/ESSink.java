package com.github.yafeiwang1240.sparkoperator.output;

import com.github.yafeiwang1240.Function;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * output to es
 * @author wangyafei
 */
public class ESSink implements Function {

    static {
        System.setProperty("es.set.netty.runtime.available.processors", "false");
        System.setProperty("io.netty.noUnsafe", "false");
    }

    @Override
    public void function() {

        try(SparkSession session = SparkSession.builder().appName("esSink")
                .enableHiveSupport().getOrCreate()) {
            org.apache.spark.sql.Dataset<Row> ds = session.sql("select c_name as name, c_workyear as value, '20200930' as p_date from user_c limit 100");
            ds.foreachPartition(new ForeachPartitionFunction<Row>() {
                @Override
                public void call(Iterator<Row> t) throws Exception {
                    try (TransportClient client = new PreBuiltTransportClient(Settings.EMPTY)
                            .addTransportAddress(new TransportAddress(InetAddress.getByName("10.110.30.3"), 9300))){
                        BulkRequestBuilder bulk = client.prepareBulk();
                        while (t.hasNext()) {
                            Row row = t.next();
                            bulk.add(new IndexRequest("index_hive", "_doc")
                                    .source(rowToMap(row)));
                            if (bulk.request().numberOfActions() >= 1024) {
                                bulk.get();
                                bulk = client.prepareBulk();
                            }
                        }
                        if (bulk.request().numberOfActions() > 0) {
                            bulk.get();
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
                .addTransportAddress(new TransportAddress(InetAddress.getByName("10.110.30.3"), 9300));
        Map<String, Object> map = new HashMap<>(3);
        map.put("name", "wangyafei");
        map.put("value", 1);
        map.put("p_date", "20200930");
        client.index(new IndexRequest("index_hive", "_doc")
                .source(map)).actionGet();
        client.close();
    }
}
