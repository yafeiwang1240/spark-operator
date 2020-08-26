package com.github.yafeiwang1240.sparkoperator.transformation.value;

import com.github.yafeiwang1240.Function;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * 说明：spark算子，cartesian
 * 特点：输入分区与输出分区多对一型
 * 执行：cartesian算子在executor端执行
 * sql: 无条件join
 * @author wangyafei
 */
public class Cartesian implements Function {

    @Override
    public void function() {
        SparkConf sparkConf = new SparkConf().setAppName("cartesian");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        List<Tuple2<Integer, String>> list1 = new ArrayList<>(10);
        for (int i = 0; i < 10; i++) {
            list1.add(new Tuple2<>(i, "list1:" + i));
        }
        List<Tuple2<Integer, String>> list2 = new ArrayList<>(10);
        for (int i = 0; i < 8; i++) {
            list2.add(new Tuple2<>(i + 1, "list2:" + (i + 1)));
        }
        JavaRDD<Tuple2<Integer, String>> rdd1 = sc.parallelize(list1);
        JavaRDD<Tuple2<Integer, String>> rdd2 = sc.parallelize(list2);
        JavaPairRDD<Tuple2<Integer, String>, Tuple2<Integer, String>> newRdd = rdd1.cartesian(rdd2);
        newRdd.collect().forEach(System.out::println);
    }
}
