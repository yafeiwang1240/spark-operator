package com.github.yafeiwang1240.sparkoperator.input;

import com.github.yafeiwang1240.Function;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableSnapshotInputFormat;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;

/**
 * hbase snapshot input
 * @author wangyafei
 */
public class HFileInput implements Function {

    @Override
    public void function() {
        JavaSparkContext sc = null;
        Connection connection = null;
        Admin admin = null;
        String snapName ="ss_snapshot";
        try {
            Configuration hconf = HBaseConfiguration.create();
            System.setProperty("HADOOP_USER_NAME", "hadoop");
            hconf.set("hbase.zookeeper.quorum", "10.110.13.58,10.110.13.101,10.110.14.194");
            hconf.set("hbase.zookeeper.property.clientPort", "2181");
            hconf.set("hbase.security.authentication", "simple");
            hconf.set("zookeeper.znode.parent", "/hbase-unsecure-2");
            hconf.set("hbase.rootdir", "hdfs://lynxcluster/hbase1.2.6_2");

            hconf.setInt("hbase.client.scanner.timeout.period", 600000);
            hconf.setInt("hbase.rpc.timeout",  600000);
            hconf.setInt("hbase.client.operation.timeout", 600000);

            SparkConf sparkConf = new SparkConf().setAppName("hFileInput");
            sc = new JavaSparkContext(sparkConf);
            Scan scan = new Scan();
            scan.addFamily(Bytes.toBytes("f"));
            scan.setCacheBlocks(false);
            hconf.set(TableInputFormat.SCAN, convertScanToString(scan));
            Job job = Job.getInstance(hconf);
            Path path = new Path("hdfs:///tmp/snapshot");

            connection = ConnectionFactory.createConnection(hconf);
            admin = connection.getAdmin();
            admin.snapshot(snapName, TableName.valueOf("ss"));
            TableSnapshotInputFormat.setInput(job, snapName, path);

            JavaPairRDD<ImmutableBytesWritable, Result> newAPIHadoopRDD = sc.newAPIHadoopRDD(
                    job.getConfiguration(), TableSnapshotInputFormat.class,
                    ImmutableBytesWritable.class,Result.class);
            JavaRDD<Map<String, String>> rdd = newAPIHadoopRDD
                    .values()
                    .map(new org.apache.spark.api.java.function.Function<Result, Map<String, String>>() {
                @Override
                public Map<String, String> call(Result v1) throws Exception {
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
            if (admin != null) {
                try {
                    admin.close();
                } catch (IOException e) {
                }
                try {
                    admin.deleteSnapshot(snapName);
                } catch (Exception e) {}
            }
            if (connection != null) {
                try {
                    connection.close();
                } catch (IOException e) {
                }
            }
        }
    }

    protected Map<String, String> resultToMap(String cf, Result result) {
        NavigableMap<byte[], NavigableMap<byte[], byte[]>> noVersionMap = result.getNoVersionMap();
        if (noVersionMap == null) return null;
        NavigableMap<byte[], byte[]> resMap = noVersionMap.get(cf.getBytes());
        Map<String, String> valueMap = new HashMap<>(resMap.size());
        for (Map.Entry<byte[],  byte[]> entry : resMap.entrySet()) {
            valueMap.put(Bytes.toString(entry.getKey()), Bytes.toString(entry.getValue()));
        }
        return valueMap;
    }

    protected String convertScanToString(Scan scan) throws IOException {
        ClientProtos.Scan proto = ProtobufUtil.toScan(scan);
        return Base64.encodeBytes(proto.toByteArray());
    }
}
