package com.github.yafeiwang1240.sparkoperator.transformation.value;

import com.github.yafeiwang1240.Function;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

/**
 * 说明：spark算子，subtract
 * 特点：输出分区是输入分区的子集
 * 执行：subtract算子在executor端执行
 * sql: where not in
 * @author wangyafei
 */
public class Subtract implements Function {

    @Override
    public void function() {
        SparkConf sparkConf = new SparkConf().setAppName("subtract");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        List<Integer> list1 = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8);
        List<Integer> list2 = Arrays.asList(5, 6, 7, 8);
        JavaRDD<Integer> rdd1 = sc.parallelize(list1);
        JavaRDD<Integer> rdd2 = sc.parallelize(list2);
        JavaRDD<Integer> newRdd = rdd1.subtract(rdd2);
        newRdd.collect().stream().forEach(System.out::println);
    }
}
