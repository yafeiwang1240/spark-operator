package com.github.yafeiwang1240.sparkoperator.input;

import com.github.yafeiwang1240.Function;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.orc.mapred.OrcStruct;
import org.apache.orc.mapreduce.OrcInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * spark读取orc文件
 * @author wangyafei
 */
public class OrcInput implements Function {

    @Override
    public void function() {
        session();
    }

    private void context() {
        JavaSparkContext sc = null;
        try {
            SparkConf sparkConf = new SparkConf().setAppName("orcInput");
            sc = new JavaSparkContext(sparkConf);
            JavaPairRDD<NullWritable, OrcStruct> orc = sc.newAPIHadoopFile("/user/hive/warehouse/pub_insert_test",
                    OrcInputFormat.class, NullWritable.class, OrcStruct.class, new Configuration());
            orc.values().foreach(v -> System.out.println(v));
        } finally {
            if (sc != null) {
                sc.close();
            }
        }
    }

    private void session() {
        SparkSession session = null;
        try {
            session = SparkSession.builder().appName("orcInput")
                    .enableHiveSupport().getOrCreate();
            Dataset<Row> orc = session.read().orc("/user/hive/warehouse/pub_insert_test");
            orc.toJavaRDD().foreach(v -> System.out.println(v));
        } finally {
            if (session != null) {
                session.close();
            }
        }
    }
}
