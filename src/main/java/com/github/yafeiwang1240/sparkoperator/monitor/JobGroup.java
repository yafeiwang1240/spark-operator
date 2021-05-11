package com.github.yafeiwang1240.sparkoperator.monitor;

import com.github.yafeiwang1240.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * job
 * @author wangyafei
 */
public class JobGroup implements Function {
    @Override
    public void function() {
        SparkSession session = null;
        try {
            session = SparkSession.builder().appName("JobGroup")
                    .enableHiveSupport().getOrCreate();
            session.sparkContext().setJobGroup("test_snapshot", "this is jobGroup test_snapshot", true);
            Dataset<Row> sql = session.sql("select * from test_snapshot");
            System.out.println("-----------test_snapshot-------------" + sql.count() + "-----------------------");
            session.sparkContext().setJobGroup("test_snapshot_recovery", "this is jobGroup test_snapshot_recovery", true);
            sql = session.sql("select * from test_snapshot_recovery");
            System.out.println("-----------test_snapshot_recovery-------------" + sql.count() + "-----------------------");
        } finally {
            session.close();
        }
    }
}
