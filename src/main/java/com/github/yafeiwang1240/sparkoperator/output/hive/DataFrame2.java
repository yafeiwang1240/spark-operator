package com.github.yafeiwang1240.sparkoperator.output.hive;

import com.github.yafeiwang1240.Function;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;
import java.util.Objects;

/**
 * RDD to hive by dataFrame
 * @author wangyafei
 */
public class DataFrame2 implements Function {
    @Override
    public void function() {
        SparkSession session = null;
        try {
            session = SparkSession.builder().appName("dataFrame")
                    .enableHiveSupport().getOrCreate();
            org.apache.spark.sql.Dataset<Row> ds = session.sql("select c_name as name, c_workyear as value from user_c limit 100");
            JavaRDD<KeyValue> rows = ds.toJavaRDD().map(KeyValue::new);
            RDD<KeyValue> rdd = rows.filter(Objects::nonNull).rdd();
            session.createDataFrame(rdd, KeyValue.class).registerTempTable("temp_pub_insert_test");
            session.sql("insert overwrite table pub_insert_test partition(p_date = '20200914') select * from temp_pub_insert_test");
        } finally {
            if (session != null) {
                session.close();
            }
        }
    }

    public static class KeyValue implements Serializable {
        private String key;
        private Integer value;

        public KeyValue(Row v1) {
            value = v1.getAs("value");
            key = v1.getAs("name");
        }

        public String getKey() {
            return key;
        }

        public void setKey(String key) {
            this.key = key;
        }

        public Integer getValue() {
            return value;
        }

        public void setValue(Integer value) {
            this.value = value;
        }
    }
}
