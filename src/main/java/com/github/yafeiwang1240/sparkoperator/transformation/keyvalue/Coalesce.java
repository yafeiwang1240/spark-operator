package com.github.yafeiwang1240.sparkoperator.transformation.keyvalue;

import com.github.yafeiwang1240.Function;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Iterator;

/**
 * 说明：spark算子，coalesce
 * 特点：单个RDD聚集
 * 执行：coalesce算子在executor端执行, 不触发shuffle write
 * sql: 无
 * @author wangyafei
 */
public class Coalesce implements Function {

    @Override
    public void function() {
        SparkSession session = null;
        try {
            session = SparkSession.builder().appName("Coalesce")
                    .enableHiveSupport().getOrCreate();
            org.apache.spark.sql.Dataset<Row> ds = session.sql("select * from repartition_test");
            ds.coalesce(2).foreachPartition(new ForeachPartitionFunction<Row>() {
                @Override
                public void call(Iterator<Row> t) throws Exception {
                    System.out.println("----------------repartition after-----------------");
                    while (t.hasNext()) {
                        System.out.println(t.next());
                    }
                }
            });
        } finally {
            if (session != null) {
                session.close();
            }
        }
    }
}
