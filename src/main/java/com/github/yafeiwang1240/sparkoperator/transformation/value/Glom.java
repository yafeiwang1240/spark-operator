package com.github.yafeiwang1240.sparkoperator.transformation.value;

import com.github.yafeiwang1240.Function;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

/**
 * 说明：spark算子，glom
 * 特点：输入分区与输出分区一对一型
 * 执行：glom算子在executor端执行
 * sql: 相当于sql的udaf
 * @author wangyafei
 */
public class Glom implements Function {

    @Override
    public void function() {
        SparkConf sparkConf = new SparkConf().setAppName("glom");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6, 7);
        JavaRDD<Integer> rdd = sc.parallelize(list);
        JavaRDD<List<Integer>> newRdd = rdd.glom();
        newRdd.collect().stream().forEach(System.out::println);
    }
}
