package com.github.yafeiwang1240.sparkoperator.transformation.value;

import com.github.yafeiwang1240.Function;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;

import java.util.Arrays;
import java.util.List;

/**
 * 说明：spark算子，persist
 * 特点：缓存型
 * 执行：persist算子在executor端执行
 * sql:
 * @author wangyafei
 */
public class Persist implements Function {

    @Override
    public void function() {
        SparkConf sparkConf = new SparkConf().setAppName("persist");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 1, 2, 3, 5);
        JavaRDD<Integer> rdd = sc.parallelize(list);
        JavaRDD<Integer> newRdd = rdd.persist(StorageLevel.MEMORY_AND_DISK());
        newRdd.collect().stream().forEach(System.out::println);
    }
}
