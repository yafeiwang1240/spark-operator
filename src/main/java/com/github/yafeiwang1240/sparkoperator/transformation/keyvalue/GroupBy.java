package com.github.yafeiwang1240.sparkoperator.transformation.keyvalue;

import com.github.yafeiwang1240.Function;
import com.github.yafeiwang1240.sparkoperator.action.Reduce;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * 说明：spark算子，groupBy
 * 特点：根据key的hashPartition, 然后shuffle write,
 *       如果求max，sum, min等，建议使用{@link Reduce}
 * 执行：groupByKey算子在executor端执行, 会有数据在executor中移动
 * sql: group by
 * @author wangyafei
 */
public class GroupBy implements Function {

    @Override
    public void function() {
        Random random = new Random(47);
        SparkConf sparkConf = new SparkConf().setAppName("groupBy");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        List<Tuple2<String, Integer>> list = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            list.add(new Tuple2("name" + (i % 5 + random.nextInt(2)), i));
        }
        JavaPairRDD<String, Integer> rdd = sc.parallelizePairs(list);
        JavaPairRDD<String, Iterable<Tuple2<String, Integer>>> newRdd = rdd.groupBy(
                new org.apache.spark.api.java.function.Function<Tuple2<String, Integer>, String>() {
            @Override
            public String call(Tuple2<String, Integer> v1) throws Exception {
                return v1._1();
            }
        });
        System.out.println(list);
        System.out.println("-----------------------ofter--------------------------");
        newRdd.collect().stream().forEach(System.out::println);
    }
}
