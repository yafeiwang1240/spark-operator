package com.github.yafeiwang1240.sparkoperator.output.hive;

import com.github.yafeiwang1240.Function;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Objects;

/**
 * RDD to hive by dataFrame
 * @author wangyafei
 */
public class DataFrame implements Function {
    @Override
    public void function() {
        SparkSession session = null;
        try {
            session = SparkSession.builder().appName("dataFrame")
                    .enableHiveSupport().getOrCreate();
            org.apache.spark.sql.Dataset<Row> ds = session.sql("select c_name as name, c_workyear as value from user_c limit 100");
            RDD<Row> rows = ds.toJavaRDD().filter(Objects::nonNull).rdd();
            session.createDataFrame(rows, new StructType(new StructField[]{
                    new StructField("name", DataTypes.StringType, false, Metadata.empty()),
                    new StructField("value", DataTypes.IntegerType, false, Metadata.empty())
            })).registerTempTable("temp_pub_insert_test");
            session.sql("insert overwrite table pub_insert_test partition(p_date = '20200914') select * from temp_pub_insert_test");
        } finally {
            if (session != null) {
                session.close();
            }
        }
    }
}
