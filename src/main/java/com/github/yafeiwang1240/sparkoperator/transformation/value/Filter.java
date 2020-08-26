package com.github.yafeiwang1240.sparkoperator.transformation.value;

import com.github.yafeiwang1240.Function;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * 说明：spark算子，filter
 * 特点：输出分区是输入分区的子集
 * 执行：filter算子在executor端执行
 * sql: 相当于where条件
 * @author wangyafei
 */
public class Filter implements Function {

    @Override
    public void function() {
        SparkConf sparkConf = new SparkConf().setAppName("filter");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8);
        JavaRDD<Integer> rdd = sc.parallelize(list);
        JavaRDD<Integer> newRdd = rdd.filter(new org.apache.spark.api.java.function.Function<Integer, Boolean>() {
            @Override
            public Boolean call(Integer v1) throws Exception {
                System.out.println("---------------------------I am filter-------------");
                return v1 % 2 == 0;
            }
        });
        newRdd.collect().stream().forEach(System.out::println);
    }
}
