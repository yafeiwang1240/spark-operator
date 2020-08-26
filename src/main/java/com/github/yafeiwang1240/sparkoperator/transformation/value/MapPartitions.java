package com.github.yafeiwang1240.sparkoperator.transformation.value;

import com.github.yafeiwang1240.Function;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * 说明：spark算子，mapPartitions
 * 特点：输入分区与输出分区一对一型
 * 执行：mapPartitions算子在executor端执行
 * sql: 相当于where条件
 * @author wangyafei
 */
public class MapPartitions implements Function {
    @Override
    public void function() {
        SparkConf sparkConf = new SparkConf().setAppName("mapPartitions");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        List<List<Integer>> list = Arrays.asList(Arrays.asList(1, 2, 3, 4), Arrays.asList(5, 6 ,7, 8));
        JavaRDD<List<Integer>> rdd = sc.parallelize(list);
        JavaRDD<Integer> newRdd = rdd.mapPartitions(new FlatMapFunction<Iterator<List<Integer>>, Integer>() {
            @Override
            public Iterator<Integer> call(Iterator<List<Integer>> listIterator) throws Exception {
                System.out.println("--------------I am mapPartitions---------------------------");
                List<Integer> integers = new ArrayList<>();
                while (listIterator.hasNext()) {
                    integers.addAll(listIterator
                            .next()
                            .stream()
                            .filter(v -> v % 2 != 0)
                            .collect(Collectors.toList()));
                }
                return integers.iterator();
            }
        });
        newRdd.collect().stream().forEach(System.out::println);
    }
}
