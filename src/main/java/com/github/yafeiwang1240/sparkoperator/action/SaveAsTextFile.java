package com.github.yafeiwang1240.sparkoperator.action;

import com.github.yafeiwang1240.Function;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

/**
 * 说明：spark算子，saveAsTextFile
 * 特点：触发job
 * 执行：map算子在executor端执行
 * sql: save
 * @author wangyafei
 */
public class SaveAsTextFile implements Function {

    @Override
    public void function() {
        SparkConf sparkConf = new SparkConf().setAppName("saveAsTextFile");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6, 7);
        JavaRDD<Integer> rdd = sc.parallelize(list);
        rdd.saveAsTextFile("/tmp/sparkoperator");
    }
}
