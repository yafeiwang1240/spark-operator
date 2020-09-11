package com.github.yafeiwang1240.sparkoperator.action;

import com.github.yafeiwang1240.Function;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

/**
 * 说明：spark算子，sortBy
 * 特点：涉及数据在partition之前传输
 * 执行：sortBy算子在executor端执行
 * sql: sort by
 * @author wangyafei
 */
public class SortBy implements Function {

    @Override
    public void function() {
        SparkConf sparkConf = new SparkConf().setAppName("sortBy");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        List<Integer> list = Arrays.asList(1, 3, 2, 5, 4, 8, 7, 6);
        JavaRDD<Integer> rdd = sc.parallelize(list);
        JavaRDD<Integer> newRdd = rdd.sortBy(new org.apache.spark.api.java.function.Function<Integer, Integer>() {
            @Override
            public Integer call(Integer v1) throws Exception {
                return v1;
            }
        }, false, 3);
        newRdd.collect().stream().forEach(System.out::println);
    }
}
