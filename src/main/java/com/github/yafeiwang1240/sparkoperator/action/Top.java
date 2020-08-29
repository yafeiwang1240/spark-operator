package com.github.yafeiwang1240.sparkoperator.action;

import com.github.yafeiwang1240.Function;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

/**
 * 说明：spark算子，top
 * 特点：触发job
 * 执行：top算子在executor端执行，汇总到driver
 * sql: sort by key limit
 * @author wangyafei
 */
public class Top implements Function {

    @Override
    public void function() {
        SparkConf sparkConf = new SparkConf().setAppName("top");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        List<Integer> list = Arrays.asList(1, 2, 6, 4, 3, 8, 9, 10, 12, 11, 9, 4, 5, 6, 7);
        JavaRDD<Integer> rdd = sc.parallelize(list);
        rdd.top(3).stream().forEach(System.out::println);
        rdd.take(5).stream().forEach(System.out::println);
        rdd.takeOrdered(5).stream().forEach(System.out::println);
        System.out.println(rdd.first());
    }
}
