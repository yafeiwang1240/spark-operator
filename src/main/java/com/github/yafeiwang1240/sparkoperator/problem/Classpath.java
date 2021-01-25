package com.github.yafeiwang1240.sparkoperator.problem;

import com.github.yafeiwang1240.Function;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.net.URL;
import java.util.Iterator;

/**
 * classpath test
 * <p>
 *     spark-submit --master local[2] --class com.github.yafeiwang1240.Job --files /etc/hbase/conf/hbase-site.xml spark-operator-1.0-SNAPSHOT.jar
 *         driver.hbase-site-self=null
 *         driver.hbase-site.xml=null
 *         dirver.classloader={@link org.apache.spark.util.MutableURLClassLoader}
 *
 *         executor.hbase-site-self=null
 *         executor.hbase-site.xml=null
 *         executor.classloader={@link org.apache.spark.util.MutableURLClassLoader}
 * </p>
 * <p>
 *     spark-submit --master yarn --class com.github.yafeiwang1240.Job --files /etc/hbase/conf/hbase-site.xml spark-operator-1.0-SNAPSHOT.jar
 *         driver.hbase-site-self=null
 *         driver.hbase-site.xml=null
 *         dirver.classloader={@link org.apache.spark.util.MutableURLClassLoader}
 *
 *         executor.hbase-site-self=I am fun
 *         executor.hbase-site.xml=/data/hadoop/yarn/local/usercache/hadoop/appcache/application_1599209875238_17162/container_e56_1599209875238_17162_01_000004/hbase-site.xml
 *         executor.classloader={@link org.apache.spark.util.MutableURLClassLoader}
 * </p>
 * <p>
 *     spark-submit --master yarn --deploy-mode cluster --class com.github.yafeiwang1240.Job --files /etc/hbase/conf/hbase-site.xml spark-operator-1.0-SNAPSHOT.jar
 *         driver.hbase-site-self=I am fun
 *         driver.hbase-site.xml=/data/hadoop/yarn/local/usercache/hadoop/appcache/application_1599209875238_17162/container_e56_1599209875238_17162_01_000002/hbase-site.xml
 *         dirver.classloader={@link org.apache.spark.util.MutableURLClassLoader}
 *
 *         executor.hbase-site-self=I am fun
 *         executor.hbase-site.xml=/data/hadoop/yarn/local/usercache/hadoop/appcache/application_1599209875238_17162/container_e56_1599209875238_17162_01_000004/hbase-site.xml
 *         executor.classloader={@link org.apache.spark.util.MutableURLClassLoader}
 * </p>
 * @author wangyafei
 */
public class Classpath implements Function {

    @Override
    public void function() {
        SparkSession session = null;
        try {
            session = SparkSession.builder().appName("classpath")
                    .enableHiveSupport().getOrCreate();
            org.apache.spark.sql.Dataset<Row> ds = session.sql("select * from repartition_test");
            System.out.println("------driver classloader = "  + HBaseConfiguration.class.getClassLoader().getClass().getName());
            URL resource = HBaseConfiguration.class.getClassLoader().getResource("hbase-site.xml");
            System.out.println("------driver hbase-site.xml "  + (resource == null ? "null" : resource.getPath()));
            System.out.println(">>>>>>>driver>>>>>>>>" + HBaseConfiguration.create().get("hbase-site-self"));
            ds.repartition(2).foreachPartition(new ForeachPartitionFunction<Row>() {
                @Override
                public void call(Iterator<Row> t) throws Exception {
                    System.out.println("------executor classloader = "  + HBaseConfiguration.class.getClassLoader().getClass().getName());
                    URL resource = HBaseConfiguration.class.getClassLoader().getResource("hbase-site.xml");
                    System.out.println("------executor hbase-site.xml "  + (resource == null ? "null" : resource.getPath()));
                    System.out.println(">>>>>>>executor>>>>>>>>" + HBaseConfiguration.create().get("hbase-site-self"));
                    System.out.println("----------------repartition after-----------------");
                    while (t.hasNext()) {
                        System.out.println(t.next());
                    }
                }
            });
        } finally {
            if (session != null) {
                session.close();
            }
        }
    }
}
