package com.github.yafeiwang1240.sparkoperator.transformation.keyvalue;

import com.github.yafeiwang1240.Function;
import org.apache.spark.HashPartitioner;
import org.apache.spark.Partitioner;
import org.apache.spark.RangePartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.rdd.RDD;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * 说明：spark算子，partitionBy
 * 特点：单个RDD聚集
 * 执行：partitionBy算子在executor端执行分区
 * sql: 开窗
 * @author wangyafei
 */
public class PartitionBy implements Function {

    @Override
    public void function() {
        customPartition();
    }

    /**
     * hash partition
     */
    protected void hashPartition() {
        Random random = new Random(47);
        SparkConf sparkConf = new SparkConf().setAppName("HashPartitioner");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        List<Tuple2<String, Integer>> list = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            list.add(new Tuple2("name" + (i % 5 + random.nextInt(2)), i));
        }
        JavaPairRDD<String, Integer> rdd = sc.parallelizePairs(list);
        JavaPairRDD<String, Integer> newRdd = rdd.partitionBy(new HashPartitioner(3));
        newRdd.collect().stream().forEach(System.out::println);
    }

    /**
     * range partition
     */
    protected void rangePartition() {
        SparkConf sparkConf = new SparkConf().setAppName("RangePartitioner");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        List<Tuple2<Integer, String>> list = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            list.add(new Tuple2((i % 5), i + 1 + "val"));
        }
        JavaPairRDD<Integer, String> pairRdd = sc.parallelizePairs(list);
        RDD<Tuple2<Integer, String>> rdd = JavaPairRDD.toRDD(pairRdd);
        RangePartitioner rangePartitioner = new RangePartitioner(5, rdd, true,
                scala.math.Ordering.Int$.MODULE$ , scala.reflect.ClassTag$.MODULE$.apply(Integer.class));
        JavaPairRDD<Integer, String> rangePair = pairRdd.partitionBy(rangePartitioner);
        rangePair.foreach(new VoidFunction<Tuple2<Integer, String>>() {
            @Override
            public void call(Tuple2<Integer, String> integerIntegerTuple2) throws Exception {
                System.out.println(integerIntegerTuple2);
            }
        });
    }

    /**
     * 自定义partition
     */
    protected void customPartition() {
        SparkConf sparkConf = new SparkConf().setAppName("CustomPartition");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        List<Tuple2<Integer, String>> list = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            list.add(new Tuple2((i % 5), i + 1 + "val"));
        }
        JavaPairRDD<Integer, String> pairRdd = sc.parallelizePairs(list);
        JavaPairRDD<Integer, String> integerStringJavaPairRDD = pairRdd.partitionBy(new Partitioner() {
            @Override
            public int numPartitions() {
                return 10;
            }

            @Override
            public int getPartition(Object key) {
                if (key == null) {
                    return 0;
                }
                if (key instanceof Integer) {
                    return ((int) key) % 5;
                }
                return 1;
            }
        });
        integerStringJavaPairRDD.foreach(new VoidFunction<Tuple2<Integer, String>>() {
            @Override
            public void call(Tuple2<Integer, String> integerStringTuple2) throws Exception {
                System.out.println(integerStringTuple2);
            }
        });
    }
}
