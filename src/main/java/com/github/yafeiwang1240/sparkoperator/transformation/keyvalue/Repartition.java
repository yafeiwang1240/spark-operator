package com.github.yafeiwang1240.sparkoperator.transformation.keyvalue;

import com.github.yafeiwang1240.Function;
import org.apache.spark.HashPartitioner;
import org.apache.spark.Partitioner;
import org.apache.spark.RangePartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

/**
 * 说明：spark算子，repartition
 * 特点：key为partition的index值，index从2开始
 * 执行：repartition算子在executor端执行，触发shuffle write
 * sql: 无
 * @author wangyafei
 */
public class Repartition implements Function {

    @Override
    public void function() {
        SparkSession session = null;
        try {
            session = SparkSession.builder().appName("repartition")
                    .enableHiveSupport().getOrCreate();
            org.apache.spark.sql.Dataset<Row> ds = session.sql("select * from repartition_test");
            ds.repartition(2).foreachPartition(new ForeachPartitionFunction<Row>() {
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
