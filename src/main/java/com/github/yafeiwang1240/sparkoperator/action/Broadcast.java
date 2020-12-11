package com.github.yafeiwang1240.sparkoperator.action;

import com.github.yafeiwang1240.Function;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * 说明：spark算子，broadcast
 * 特点：触发job
 * 执行：在hql中，not in(子查询)，会将主查询和子查询中结果小的发到driver端，然后broadcast
 * sql: not in(select ...), in(select )
 * 注：not in或者in子查询慎用，使用left join实现
 * @author wangyafei
 */
public class Broadcast implements Function {

    @Override
    public void function() {
        SparkConf sparkConf = new SparkConf().setAppName("broadcast");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 4, 4, 3, 8, 9, 7, 12, 1, 9, 4, 5, 6, 7));
        JavaRDD<Integer> repartition = rdd.repartition(3);
        List<Integer> list = Arrays.asList(1, 4, 7);
        org.apache.spark.broadcast.Broadcast<List<Integer>> broadcast = sc.broadcast(list);
        JavaRDD<Integer> filter = repartition.filter(new org.apache.spark.api.java.function.Function<Integer, Boolean>() {
            protected Set<Integer> filter = new HashSet<>(broadcast.getValue());

            @Override
            public Boolean call(Integer v1) throws Exception {
                return !filter.contains(v1);
            }
        });
        filter.collect().stream().forEach(v -> System.out.println(v));
    }
}
