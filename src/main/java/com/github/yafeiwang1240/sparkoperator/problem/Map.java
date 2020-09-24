package com.github.yafeiwang1240.sparkoperator.problem;

import com.github.yafeiwang1240.Function;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;
import java.util.Objects;

/**
 * error
 * <p>
 *     同一个lambda函数式在不同数据类型的地方使用，就会报错{@link #functionError()}
 * </p>
 * 原因就是spark在执行第二个相同的算子时，调用了第一个函数，这个是scala在做eta-conversion
 *   的反过程eta-expansion, 默认第二个算子函数和第一个相同，并且设置了参数类型
 *
 * no error
 * <p>
 *     自定义lambda不会报错{@link #functionLambda()} ()}
 * </p>
 *
 * @author wangyafei
 */
public class Map implements Function {

    @Override
    public void function() {
        functionError();
    }

    protected void functionError() {
        SparkSession session = null;
        try {
            session = SparkSession.builder().appName("map")
                    .enableHiveSupport().getOrCreate();
            org.apache.spark.sql.Dataset<Row> ds = session.sql("select c_name as name, c_workyear as value from user_c limit 100");
            JavaRDD<KeyValue> rows = ds.toJavaRDD().filter(v1 -> Objects.nonNull(v1)).map(KeyValue::new);
            RDD<KeyValue> rdd = rows.filter(v1 -> Objects.nonNull(v1)).map(KeyValue::new).rdd();
            System.out.println("----------rdd---------" + rdd.count());
        } finally {
            if (session != null) {
                session.close();
            }
        }
    }

    /**
     * lambda同一函数是ok
     */
    protected void functionLambda() {
        SparkSession session = null;
        try {
            session = SparkSession.builder().appName("map")
                    .enableHiveSupport().getOrCreate();
            org.apache.spark.sql.Dataset<Row> ds = session.sql("select c_name as name, c_workyear as value from user_c limit 100");
            JavaRDD<KeyValue> rows = ds.toJavaRDD().filter(v1 -> Objects.nonNull(v1)).map(v1 -> new KeyValue(v1));
            RDD<KeyValue> rdd = rows.filter(v1 -> Objects.nonNull(v1)).map(v1 -> new KeyValue(v1)).rdd();
            System.out.println("----------rdd---------" + rdd.count());
        } finally {
            if (session != null) {
                session.close();
            }
        }
    }

    public static class KeyValue implements Serializable {
        private String key;
        private Integer value;

        public KeyValue(Object obj) {
            if (obj instanceof KeyValue) {
                KeyValue keyValue = (KeyValue) obj;
                key = keyValue.getKey();
                value = keyValue.getValue() + 1;
            } else if (obj instanceof Row){
                Row v1 = (Row) obj;
                value = v1.getAs("value");
                key = v1.getAs("name");
            } else {
                throw new IllegalArgumentException("错误的类型：" + obj.getClass());
            }
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
