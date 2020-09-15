package com.github.yafeiwang1240.sparkoperator.output;

import com.github.yafeiwang1240.Function;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * dataset output
 * @author wangyafei
 */
public class PrintSink implements Function {
    @Override
    public void function() {
        SparkConf sparkConf = new SparkConf().setAppName("printSink");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        List<Tuple2<String, Integer>> list = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            list.add(new Tuple2("n" + i, i));
        }
        JavaRDD<Tuple2<String, Integer>> rdd = sc.parallelize(list);
        JavaRDD<Row> rowRDD = rdd.map(new org.apache.spark.api.java.function.Function<Tuple2<String, Integer>, Row>() {
            @Override
            public Row call(Tuple2<String, Integer> v1) throws Exception {
                return RowFactory.create(v1._1(), v1._2());
            }
        });
        SparkSession session = new SparkSession(sc.sc());
        session.createDataset(rowRDD.rdd(), RowEncoder.apply(new StructType(new StructField[]{
                new StructField("name", DataTypes.StringType, false, Metadata.empty()),
                new StructField("value", DataTypes.IntegerType, false, Metadata.empty())
        }))).foreachPartition(new ForeachPartitionFunction<Row>() {
            @Override
            public void call(Iterator<Row> t) throws Exception {
                while (t.hasNext()) {
                    System.out.println(t.next().toString());
                }
            }
        });
        session.close();
    }
}
