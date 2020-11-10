package com.github.yafeiwang1240.sparkoperator.output.hive;

import com.github.yafeiwang1240.Function;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import scala.Tuple2;

import java.util.*;

/**
 * output to hbase
 * @author wangyafei
 */
public class HBaseSink implements Function {

    @Override
    public void function() {
        try(SparkSession session = SparkSession.builder().appName("hbaseSink")
                .enableHiveSupport().getOrCreate()) {
            org.apache.spark.sql.Dataset<Row> ds = session.sql("select user_id as id, c_workyear as value from user_c limit 100");
            String[] fieldNames = ds.schema().fieldNames();
            Arrays.sort(fieldNames);
            JavaPairRDD<ImmutableBytesWritable, KeyValue> flatMapToPair = ds.toJavaRDD().mapToPair(new PairFunction<Row, ImmutableBytesWritable, List<Tuple2<ImmutableBytesWritable, KeyValue>>>() {
                @Override
                public Tuple2<ImmutableBytesWritable, List<Tuple2<ImmutableBytesWritable, KeyValue>>> call(Row row) throws Exception {
                    List<Tuple2<ImmutableBytesWritable, KeyValue>> kvs = new ArrayList<>();
                    byte[] rowKey = Bytes.toBytes(row.get(row.fieldIndex("id")).toString());
                    ImmutableBytesWritable writable = new ImmutableBytesWritable(rowKey);
                    for (int i = 0; i < fieldNames.length; i++) {
                        Object v = row.get(row.fieldIndex(fieldNames[i]));
                        if (v == null) {
                            continue;
                        }
                        kvs.add(new Tuple2<>(writable, new KeyValue(rowKey, "f".getBytes(), Bytes.toBytes(fieldNames[i]), Bytes.toBytes(v.toString()))));
                    }
                    return new Tuple2<>(writable, kvs);
                }
            }).sortByKey().flatMapToPair(new PairFlatMapFunction<Tuple2<ImmutableBytesWritable, List<Tuple2<ImmutableBytesWritable, KeyValue>>>, ImmutableBytesWritable, KeyValue>() {
                @Override
                public Iterator<Tuple2<ImmutableBytesWritable, KeyValue>> call(Tuple2<ImmutableBytesWritable, List<Tuple2<ImmutableBytesWritable, KeyValue>>> tuple2) throws Exception {
                    return tuple2._2().iterator();
                }
            });
            String tableName = "bulkload";
            Configuration config = HBaseConfiguration.create();
            config.set("hbase.zookeeper.quorum", "10.110.13.58,10.110.13.101,10.110.14.194");
            config.set("hbase.zookeeper.property.clientPort", "2181");
            config.set("hbase.security.authentication", "simple");
            config.set("zookeeper.znode.parent", "/hbase-unsecure-2");
            config.setInt("hbase.client.scanner.timeout.period", 600000);
            config.setInt("hbase.rpc.timeout",  600000);
            config.setInt("hbase.client.operation.timeout", 600000);
            Connection connection = ConnectionFactory.createConnection(config);
            Table table = connection.getTable(TableName.valueOf(tableName));
            Job job = Job.getInstance(config);
            job.setMapOutputKeyClass(ImmutableBytesWritable.class);
            job.setMapOutputValueClass(KeyValue.class);

            HRegionLocator regionLocator = new HRegionLocator(TableName.valueOf(tableName), (ClusterConnection) connection);
            HFileOutputFormat2.configureIncrementalLoad(job, table, regionLocator); // HFileOutputFormat2.configureIncrementalLoadMap(job, table);

            String temp = "/tmp/bulkload/" + tableName + "_" + System.currentTimeMillis();
            config.set("hfile.compression", "snappy");
            flatMapToPair.saveAsNewAPIHadoopFile(temp, ImmutableBytesWritable.class, KeyValue.class, HFileOutputFormat2.class, config);
            LoadIncrementalHFiles loader = new LoadIncrementalHFiles(config);
            loader.doBulkLoad(new Path(temp), connection.getAdmin(), table, regionLocator);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
