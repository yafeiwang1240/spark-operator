package com.github.yafeiwang1240.sparkoperator.transformation.value;

import com.github.yafeiwang1240.Function;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

/**
 * 说明：spark算子，takeSample
 * 特点：输出分区是输入分区的子集
 * 执行：takeSample算子在executor端执行
 * sql:
 * @author wangyafei
 */
public class TakeSample implements Function {

    @Override
    public void function() {
        SparkConf sparkConf = new SparkConf().setAppName("takeSample");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 1, 2, 3, 5);
        JavaRDD<Integer> rdd = sc.parallelize(list);
        List<Integer> newList = rdd.takeSample(true, 3, 47);
        newList.stream().forEach(System.out::println);
    }
}
