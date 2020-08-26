package com.github.yafeiwang1240.sparkoperator.transformation.value;

import com.github.yafeiwang1240.Function;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

/**
 * 说明：spark算子，union
 * 特点：输入分区与输出分区多对一型
 * 执行：union算子在executor端执行
 * sql: rdd相加
 * @author wangyafei
 */
public class Union implements Function {

    @Override
    public void function() {
        SparkConf sparkConf = new SparkConf().setAppName("union");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        List<Integer> list1 = Arrays.asList(1, 2, 3, 4, 5);
        List<Integer> list2 = Arrays.asList(1, 6, 7, 8, 9);
        JavaRDD<Integer> rdd1 = sc.parallelize(list1);
        JavaRDD<Integer> rdd2 = sc.parallelize(list2);
        JavaRDD<Integer> newRdd = rdd1.union(rdd2);
        newRdd.collect().stream().forEach(System.out::println);
    }
}
