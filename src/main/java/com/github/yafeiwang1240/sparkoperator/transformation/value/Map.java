package com.github.yafeiwang1240.sparkoperator.transformation.value;

import com.github.yafeiwang1240.Function;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

/**
 * 说明：spark算子，map
 * 特点：输入分区与输出分区一对一型
 * 执行：map算子在executor端执行
 * sql: 相当于sql的select a as b
 * @author wangyafei
 */
public class Map implements Function {

    @Override
    public void function() {
        SparkConf sparkConf = new SparkConf().setAppName("map");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6, 7);
        JavaRDD<Integer> rdd = sc.parallelize(list);
        JavaRDD<Integer> newRdd = rdd.map(new org.apache.spark.api.java.function.Function<Integer, Integer>() {
            @Override
            public Integer call(Integer v1) throws Exception {
                System.out.println("--------------I am map---------------------------");
                return v1 + 1;
            }
        });
        newRdd.collect().stream().forEach(System.out::println);
    }
}
