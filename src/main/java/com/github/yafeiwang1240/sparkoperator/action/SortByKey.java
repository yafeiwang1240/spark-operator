package com.github.yafeiwang1240.sparkoperator.action;

import com.github.yafeiwang1240.Function;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * 说明：spark算子，sortByKey
 * 特点：涉及数据在partition中间传输
 * 执行：sortByKey算子在executor端执行
 * sql: sortByKey
 * @author wangyafei
 */
public class SortByKey implements Function {

    @Override
    public void function() {
        SparkConf sparkConf = new SparkConf().setAppName("sortByKey");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        List<Tuple2<String, Integer>> list = new ArrayList<>();
        for (int i = 0; i < 30; i++) {
            list.add(new Tuple2("name" + i, i));
        }
        JavaPairRDD<String, Integer> rdd = sc.parallelizePairs(list);
        JavaPairRDD<String, Integer> newRdd = rdd.sortByKey(new ComparatorSelf());
        newRdd.collect().stream().forEach(System.out::println);
    }

    public static class ComparatorSelf implements Comparator<String>, Serializable {
        @Override
        public int compare(String o1, String o2) {
            if (o1.length() == o2.length()) {
                return o2.compareTo(o1);
            }
            return o2.length() - o1.length();
        }
    }
}
