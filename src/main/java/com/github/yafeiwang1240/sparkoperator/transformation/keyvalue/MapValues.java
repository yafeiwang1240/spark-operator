package com.github.yafeiwang1240.sparkoperator.transformation.keyvalue;

import com.github.yafeiwang1240.Function;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * 说明：spark算子，mapValues
 * 特点：输入分区与输出分区一对一
 * 执行：mapValues算子在executor端执行
 * sql: change
 * @author wangyafei
 */
public class MapValues implements Function {

    @Override
    public void function() {
        SparkConf sparkConf = new SparkConf().setAppName("mapValues");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        List<Tuple2<String, Integer>> list = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            list.add(new Tuple2("name" + i, i));
        }
        JavaPairRDD<String, Integer> rdd = sc.parallelizePairs(list);
        JavaPairRDD<String, Integer> newRdd = rdd.mapValues(new org.apache.spark.api.java.function.Function<Integer, Integer>() {
            @Override
            public Integer call(Integer v1) throws Exception {
                System.out.println("--------------------------I am mapValues--------------");
                return ++v1;
            }
        });
        newRdd.collect().stream().forEach(System.out::println);
    }
}
