package com.github.yafeiwang1240.sparkoperator.transformation.keyvalue;

import com.github.yafeiwang1240.Function;
import org.apache.spark.HashPartitioner;
import org.apache.spark.Partitioner;
import org.apache.spark.RangePartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * 说明：spark算子，repartition
 * 特点：单个RDD聚集
 * 执行：repartition算子在executor端执行分区
 * sql: 开窗
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
            ds.foreach(new ForeachFunction<Row>() {
                @Override
                public void call(Row row) throws Exception {
                    System.out.println(row);
                }
            });
            ds.repartition(2).foreach(new ForeachFunction<Row>() {
                @Override
                public void call(Row row) throws Exception {
                    System.out.println(row);
                }
            });
        } finally {
            if (session != null) {
                session.close();
            }
        }
    }
}
