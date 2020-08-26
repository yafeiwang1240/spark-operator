package com.github.yafeiwang1240.sparkoperator.transformation.value;

import com.github.yafeiwang1240.Function;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

/**
 * 说明：spark算子，group
 * 特点：输入分区与输出分区多对多型
 * 执行：map算子在executor端执行
 * sql: group by
 * @author wangyafei
 */
public class GroupBy implements Function {

    @Override
    public void function() {
        SparkConf sparkConf = new SparkConf().setAppName("group");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6, 7);
        JavaRDD<Integer> rdd = sc.parallelize(list);
        JavaPairRDD<Integer, Iterable<Integer>> newRdd = rdd.groupBy(new org.apache.spark.api.java.function.Function<Integer, Integer>() {
            @Override
            public Integer call(Integer v1) throws Exception {
                System.out.println("-------------------------I am groupBy----------------");
                return v1 % 2;
            }
        });
        newRdd.collect().stream().forEach(System.out::println);
    }
}
