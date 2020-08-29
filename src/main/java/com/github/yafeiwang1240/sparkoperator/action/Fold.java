package com.github.yafeiwang1240.sparkoperator.action;

import com.github.yafeiwang1240.Function;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import java.util.Arrays;
import java.util.List;

/**
 * 说明：spark算子，fold
 * 特点：触发job
 * 执行：fold算子在executor端执行，汇总到driver
 * sql:
 * @author wangyafei
 */
public class Fold implements Function {

    @Override
    public void function() {
        SparkConf sparkConf = new SparkConf().setAppName("fold");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        List<Integer> list = Arrays.asList(1, 2, 6, 4, 3, 8, 9, 10, 12, 11, 9, 4, 5, 6, 7);
        JavaRDD<Integer> rdd = sc.parallelize(list);
        Integer integer = rdd.fold(18, new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return Math.max(integer, integer2);
            }
        });
        System.out.println(integer);
    }
}
