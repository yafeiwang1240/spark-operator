package com.github.yafeiwang1240.sparkoperator.output.hive;

import com.github.yafeiwang1240.Function;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Objects;

/**
 * RDD to hive by overwrite
 * @author wangyafei
 */
public class Overwrite implements Function {
    @Override
    public void function() {
        SparkSession session = null;
        try {
            session = SparkSession.builder().appName("overwrite")
                    .enableHiveSupport().getOrCreate();
            Dataset<Row> ds = session.sql("select c_name as name, c_workyear as value from user_c limit 100");
            RDD<Row> rows = ds.toJavaRDD().filter(Objects::nonNull).rdd();
            session.createDataset(rows, RowEncoder.apply(new StructType(new StructField[]{
                    new StructField("name", DataTypes.StringType, false, Metadata.empty()),
                    new StructField("value", DataTypes.IntegerType, false, Metadata.empty())
            }))).write().mode(SaveMode.Overwrite)
                    .format("Hive")
                    .saveAsTable("pub_insert_test_dataset");

            session.createDataFrame(rows, new StructType(new StructField[]{
                    new StructField("name", DataTypes.StringType, false, Metadata.empty()),
                    new StructField("value", DataTypes.IntegerType, false, Metadata.empty())
            })).write().mode(SaveMode.Overwrite)
                    .format("Hive")
                    .saveAsTable("pub_insert_test_dataframe");
        } finally {
            if (session != null) {
                session.close();
            }
        }
    }
}
