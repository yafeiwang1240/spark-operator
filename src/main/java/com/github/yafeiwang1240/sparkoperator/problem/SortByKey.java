package com.github.yafeiwang1240.sparkoperator.problem;

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
 * 匿名内部类，没有实现序列化，不能使用{@link #functionSerializableError()}
 *   可以使用{@link #functionSerializable()}, {@link #functionClass()}
 *
 * lambda不可用{@link #functionLambdaError()},
 *   强转serializable{@link #functionSerializableLambda()}
 *
 * @author wangyafei
 * @date 2020-09-23
 */
public class SortByKey implements Function {

    @Override
    public void function() {
        functionClass();
    }

    /**
     * object not serializable (class: com.github.yafeiwang1240.sparkoperator.problem.SortByKey$1,
     * value: com.github.yafeiwang1240.sparkoperator.problem.SortByKey$1@3a7469ca)
     */
    protected void functionSerializableError() {
        SparkConf sparkConf = new SparkConf().setAppName("sortByKey");
        try (JavaSparkContext sc = new JavaSparkContext(sparkConf)) {
            List<Tuple2<String, Integer>> list = new ArrayList<>();
            for (int i = 0; i < 30; i++) {
                list.add(new Tuple2("name" + i, i));
            }
            JavaPairRDD<String, Integer> rdd = sc.parallelizePairs(list);
            JavaPairRDD<String, Integer> newRdd = rdd.sortByKey(new Comparator<String>() {
                @Override
                public int compare(String o1, String o2) {
                    return o2.compareTo(o1);
                }
            });
            newRdd.collect().stream().forEach(System.out::println);
        }
    }

    /**
     * object not serializable (class: com.github.yafeiwang1240.sparkoperator.problem.SortByKey$$Lambda$17/619629247,
     * value: com.github.yafeiwang1240.sparkoperator.problem.SortByKey$$Lambda$17/619629247@21540255)
     */
    protected void functionLambdaError() {
        SparkConf sparkConf = new SparkConf().setAppName("sortByKey");
        try (JavaSparkContext sc = new JavaSparkContext(sparkConf)) {
            List<Tuple2<String, Integer>> list = new ArrayList<>();
            for (int i = 0; i < 30; i++) {
                list.add(new Tuple2("name" + i, i));
            }
            JavaPairRDD<String, Integer> rdd = sc.parallelizePairs(list);
            JavaPairRDD<String, Integer> newRdd = rdd.sortByKey((o1, o2) -> o2.compareTo(o1));
            newRdd.collect().stream().forEach(System.out::println);
        }
    }

    /**
     * ok
     */
    protected void functionSerializableLambda() {
        SparkConf sparkConf = new SparkConf().setAppName("sortByKey");
        try (JavaSparkContext sc = new JavaSparkContext(sparkConf)) {
            List<Tuple2<String, Integer>> list = new ArrayList<>();
            for (int i = 0; i < 30; i++) {
                list.add(new Tuple2("name" + i, i));
            }
            JavaPairRDD<String, Integer> rdd = sc.parallelizePairs(list);
            JavaPairRDD<String, Integer> newRdd = rdd.sortByKey((ComparatorSerializable<String>) (o1, o2) -> o2.compareTo(o1));
            newRdd.collect().stream().forEach(System.out::println);
        }
    }

    /**
     * 这么做是ok的
     */
    protected void functionSerializable() {
        SparkConf sparkConf = new SparkConf().setAppName("sortByKey");
        try (JavaSparkContext sc = new JavaSparkContext(sparkConf)) {
            List<Tuple2<String, Integer>> list = new ArrayList<>();
            for (int i = 0; i < 30; i++) {
                list.add(new Tuple2("name" + i, i));
            }
            JavaPairRDD<String, Integer> rdd = sc.parallelizePairs(list);
            JavaPairRDD<String, Integer> newRdd = rdd.sortByKey(new ComparatorSerializable<String>() {
                @Override
                public int compare(String o1, String o2) {
                    return o2.compareTo(o1);
                }
            });
            newRdd.collect().stream().forEach(System.out::println);
        }
    }

    /**
     * 推荐使用这种方式
     */
    protected void functionClass() {
        SparkConf sparkConf = new SparkConf().setAppName("sortByKey");
        try (JavaSparkContext sc = new JavaSparkContext(sparkConf)) {
            List<Tuple2<String, Integer>> list = new ArrayList<>();
            for (int i = 0; i < 30; i++) {
                list.add(new Tuple2("name" + i, i));
            }
            JavaPairRDD<String, Integer> rdd = sc.parallelizePairs(list);
            JavaPairRDD<String, Integer> newRdd = rdd.sortByKey(new ComparatorSelf());
            newRdd.collect().stream().forEach(System.out::println);
        }
    }

    public interface ComparatorSerializable<T> extends Comparator<T>, Serializable {}

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
