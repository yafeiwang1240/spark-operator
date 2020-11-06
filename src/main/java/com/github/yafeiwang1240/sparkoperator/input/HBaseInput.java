package com.github.yafeiwang1240.sparkoperator.input;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.io.IOException;
import java.util.Map;

/**
 * hbase snapshot input
 * @author wangyafei
 */
public class HBaseInput extends HFileInput {

    @Override
    public void function() {
        JavaSparkContext sc = null;
        try {
            Configuration hconf = HBaseConfiguration.create();
            System.setProperty("HADOOP_USER_NAME", "hadoop");
            System.setProperty("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
            hconf.set("hbase.zookeeper.quorum", "10.110.13.58,10.110.13.101,10.110.14.194");
            hconf.set("hbase.zookeeper.property.clientPort", "2181");
            hconf.set("hbase.security.authentication", "simple");
            hconf.set("zookeeper.znode.parent", "/hbase-unsecure-2");
            hconf.set(TableInputFormat.INPUT_TABLE, "ss");

            hconf.setInt("hbase.client.scanner.timeout.period", 600000);
            hconf.setInt("hbase.rpc.timeout",  600000);
            hconf.setInt("hbase.client.operation.timeout", 600000);

            SparkConf sparkConf = new SparkConf().setAppName("hBaseInput");
            sc = new JavaSparkContext(sparkConf);

            Scan scan = new Scan();
            scan.addFamily(Bytes.toBytes("f"));
            scan.setCacheBlocks(false);
            hconf.set(TableInputFormat.SCAN, convertScanToString(scan));

            JavaPairRDD<ImmutableBytesWritable, Result> newAPIHadoopRDD = sc.newAPIHadoopRDD(
                    hconf, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);
            JavaRDD<Map<String, String>> rdd = newAPIHadoopRDD.values()
                    .map(new Function<Result, Map<String, String>>() {
                @Override
                public Map<String, String> call(Result v1) throws Exception {
                    v1.getRow();
                    return resultToMap("f", v1);
                }
            });
            rdd.collect().forEach(v -> System.out.println(v));
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            if (sc != null) {
                sc.close();
            }
        }
    }
}
