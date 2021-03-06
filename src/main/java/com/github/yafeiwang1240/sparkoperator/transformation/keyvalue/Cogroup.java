package com.github.yafeiwang1240.sparkoperator.transformation.keyvalue;

import com.github.yafeiwang1240.Function;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * 说明：spark算子，Cogroup
 * 特点：两个RDD聚集
 * 执行：Cogroup算子在executor端执行, 但是涉及rdd之间的交互
 * sql:
 * @author wangyafei
 */
public class Cogroup implements Function {

    @Override
    public void function() {
        Random random = new Random(47);
        SparkConf sparkConf = new SparkConf().setAppName("Cogroup");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        List<Tuple2<String, Integer>> list1 = new ArrayList<>();
        List<Tuple2<String, Integer>> list2 = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            list1.add(new Tuple2("name" + (i % 5 + random.nextInt(2)), i));
        }
        for (int i = 0; i < 10; i++) {
            list2.add(new Tuple2("name" + (i % 5 + random.nextInt(2)), i));
        }
        JavaPairRDD<String, Integer> rdd1 = sc.parallelizePairs(list1);
        JavaPairRDD<String, Integer> rdd2 = sc.parallelizePairs(list2);
        JavaPairRDD<String, Tuple2<Iterable<Integer>, Iterable<Integer>>> newRdd = rdd1.cogroup(rdd2);
        newRdd.collect().stream().forEach(System.out::println);
    }
}
