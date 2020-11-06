package com.github.yafeiwang1240.sparkoperator.input;

import com.github.yafeiwang1240.Function;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

/**
 * spark读取orc文件
 * @author wangyafei
 */
public class TextInput implements Function {

    @Override
    public void function() {
        context();
    }

    private void context() {
        JavaSparkContext sc = null;
        try {
            SparkConf sparkConf = new SparkConf().setAppName("textInput");
            sc = new JavaSparkContext(sparkConf);
            JavaRDD<String> orc = sc.textFile("/user/hive/warehouse/pub_insert_test");
            orc.foreach(v -> System.out.println(v));
        } finally {
            if (sc != null) {
                sc.close();
            }
        }
    }

    private void session() {
        SparkSession session = null;
        try {
            session = SparkSession.builder().appName("textInput")
                    .enableHiveSupport().getOrCreate();
            Dataset<String> orc = session.read().textFile("/user/hive/warehouse/pub_insert_test");
            orc.toJavaRDD().foreach(v -> System.out.println(v));
        } finally {
            if (session != null) {
                session.close();
            }
        }
    }
}
