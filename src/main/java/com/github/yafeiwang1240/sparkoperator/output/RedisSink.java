package com.github.yafeiwang1240.sparkoperator.output;

import com.github.yafeiwang1240.Function;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.Iterator;
import java.util.Map;

/**
 * output to redis
 * @author wangyafei
 */
public class RedisSink implements Function {

    @Override
    public void function() {

        try(SparkSession session = SparkSession.builder().appName("redisSink")
                .enableHiveSupport().getOrCreate()) {
            org.apache.spark.sql.Dataset<Row> ds = session.sql("select c_name as name, c_workyear as value from user_c limit 100");
            ds.foreachPartition(new ForeachPartitionFunction<Row>() {
                @Override
                public void call(Iterator<Row> t) throws Exception {
                    try (JedisPool pool = new JedisPool(new JedisPoolConfig(),
                            "10.110.30.1", 6379, 10000)){
                        while (t.hasNext()) {
                            Row row = t.next();
                            Jedis jedis = pool.getResource();
                            jedis.hset("user_c", row.getString(row.fieldIndex("name")),
                                    row.get(row.fieldIndex("value")).toString());
                            jedis.close();
                        }
                    }
                }
            });
        }
    }

    public static void main(String[] args) {
        JedisPool pool = new JedisPool(new JedisPoolConfig(),
                "10.110.30.1", 6379, 10000);
        Map<String,String> map = pool.getResource().hgetAll("user_c");
        System.out.println(map);
        pool.close();
    }
}
