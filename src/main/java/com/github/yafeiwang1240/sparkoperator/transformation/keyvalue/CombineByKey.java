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
 * 说明：spark算子，combineByKey
 * 特点：单个RDD聚集
 * 执行：combineByKey算子在executor端执行, 但是涉及rdd之间的交互
 * sql:
 * @author wangyafei
 */
public class CombineByKey implements Function {

    @Override
    public void function() {
        Random random = new Random(47);
        SparkConf sparkConf = new SparkConf().setAppName("combineByKey");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        List<Tuple2<String, Integer>> list = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            list.add(new Tuple2("name" + (i % 5 + random.nextInt(2)), i));
        }
        JavaPairRDD<String, Integer> rdd = sc.parallelizePairs(list);
        JavaPairRDD<String, Integer> newRdd = rdd.combineByKey(new org.apache.spark.api.java.function.Function<Integer, Integer>() {
            @Override
            public Integer call(Integer v1) throws Exception {
                System.out.println("-------------------------I am combineByKey function1-------------");
                return v1;
            }
        }, new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                System.out.println("-------------------------I am combineByKey function2-------------");
                return Math.max(v1, v2);
            }
        }, new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                System.out.println("-------------------------I am combineByKey function3-------------");
                return Math.max(v1, v2);
            }
        });
        System.out.println(list);
        System.out.println("-----------------------ofter--------------------------");
        newRdd.collect().stream().forEach(System.out::println);
    }
}
