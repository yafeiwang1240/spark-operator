package com.github.yafeiwang1240.sparkoperator.transformation.value;

import com.github.yafeiwang1240.Function;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * 说明：spark算子，flatMap
 * 特点：输入分区与输出分区一对一型
 * 执行：flatMap算子在executor端执行
 * sql: 对应sql的转换函数（udtf）
 * @author wangyafei
 */
public class FlatMap implements Function {
    @Override
    public void function() {
        SparkConf sparkConf = new SparkConf().setAppName("flatMap");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        List<List<Integer>> list = Arrays.asList(Arrays.asList(1, 2, 3, 4), Arrays.asList(5, 6 ,7, 8));
        JavaRDD<List<Integer>> rdd = sc.parallelize(list);
        JavaRDD<Integer> newRdd = rdd.flatMap(new FlatMapFunction<List<Integer>, Integer>() {
            @Override
            public Iterator<Integer> call(List<Integer> integers) throws Exception {
                System.out.println("---------------------I am flatMap--------------");
                return integers.iterator();
            }
        });
        newRdd.collect().stream().forEach(System.out::println);
    }
}
