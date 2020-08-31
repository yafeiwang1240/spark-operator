package com.github.yafeiwang1240.sparkoperator.action;

import com.github.yafeiwang1240.Function;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * 说明：spark算子，count
 * 特点：触发job
 * 执行：count算子在executor端执行，返回到driver
 * sql:
 * @author wangyafei
 */
public class Count implements Function {

    @Override
    public void function() {
        SparkConf sparkConf = new SparkConf().setAppName("count");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        List<Tuple2<String, Integer>> list = new ArrayList<>(10);
        Random random = new Random(47);
        for (int i = 0; i < 10; i++) {
            list.add(new Tuple2("name" + random.nextInt(5), i));
        }
        JavaPairRDD<String, Integer> rdd = sc.parallelizePairs(list);
        System.out.println(rdd.count());
    }
}
