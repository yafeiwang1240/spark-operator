package com.github.yafeiwang1240.sparkoperator.action;

import com.github.yafeiwang1240.Function;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

/**
 * 说明：spark算子，foreach
 * 特点：触发job
 * 执行：map算子在executor端执行
 * sql: insert
 * @author wangyafei
 */
public class Foreach implements Function {
    @Override
    public void function() {
        SparkConf sparkConf = new SparkConf().setAppName("foreach");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6, 7);
        JavaRDD<Integer> rdd = sc.parallelize(list);
        rdd.repartition(2).foreach(v -> {
            System.out.println(v);
        });
    }
}
