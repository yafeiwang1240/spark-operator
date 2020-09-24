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
 *     不同的stage也不行{@link #functionErrorStage()}
 *     不同的job也不行{@link #functionErrorJob()}
 *     使用泛型函数也会报错{@link #functionGenerics()}
 * </p>
 * 原因就是spark在执行第二个相同的算子时，调用了第一个函数，这个是scala 在做eta-conversion
 *  的反过程eta-expansion, 默认第二个算子函数和第一个相同，并且设置了参数类型
 *
 * no error
 * <p>
 *     自定义lambda不会报错{@link #functionLambda()} ()}
 *     不同的lambda不会报错{@link #functionLambda()} ()}
 *     匿名内部类不会报错{@link #functionAnon()} ()} ()}
 *     自定义模板类不会报错{@link #functionGenericClass()} ()} ()} ()}
 * </p>
 *
 * @author wangyafei
 */
public class Filter implements Function {

    @Override
    public void function() {
        functionLambdaFunction();
    }

    /**
     * java.lang.ClassCastException: com.github.yafeiwang1240.sparkoperator.problem.Filter$KeyValue
     *     cannot be cast to org.apache.spark.sql.Row
     */
    protected void functionError() {
        SparkSession session = null;
        try {
            session = SparkSession.builder().appName("filter")
                    .enableHiveSupport().getOrCreate();
            org.apache.spark.sql.Dataset<Row> ds = session.sql("select c_name as name, c_workyear as value from user_c limit 100");
            JavaRDD<KeyValue> rows = ds.toJavaRDD().filter(Objects::nonNull).map(KeyValue::new);
            RDD<KeyValue> rdd = rows.filter(Objects::nonNull).rdd();
            System.out.println("----------rdd---------" + rdd.count());
        } finally {
            if (session != null) {
                session.close();
            }
        }
    }

    /**
     * Caused by: java.lang.ClassCastException: scala.Tuple2 cannot be cast to org.apache.spark.sql.Row
     */
    protected void functionErrorStage() {
        SparkSession session = null;
        try {
            session = SparkSession.builder().appName("filter")
                    .enableHiveSupport().getOrCreate();
            org.apache.spark.sql.Dataset<Row> ds = session.sql("select c_name as name, c_workyear as value from user_c limit 100");
            JavaRDD<KeyValue> rows = ds.toJavaRDD().filter(Objects::nonNull).map(KeyValue::new);
            JavaPairRDD<Integer, Iterable<KeyValue>> rdd = rows.groupBy(new org.apache.spark.api.java.function.Function<KeyValue, Integer>() {
                @Override
                public Integer call(KeyValue v1) throws Exception {
                    return v1.getValue();
                }
            }).filter(Objects::nonNull);
            System.out.println("----------rdd---------" + rdd.count());
        } finally {
            if (session != null) {
                session.close();
            }
        }
    }

    /**
     * Caused by: java.lang.ClassCastException: com.github.yafeiwang1240.sparkoperator.problem.Filter$KeyValue
     *   cannot be cast to org.apache.spark.sql.Row
     */
    protected void functionErrorJob() {
        SparkSession session = null;
        try {
            session = SparkSession.builder().appName("filter")
                    .enableHiveSupport().getOrCreate();
            org.apache.spark.sql.Dataset<Row> ds = session.sql("select c_name as name, c_workyear as value from user_c limit 100");
            JavaRDD<KeyValue> rows = ds.toJavaRDD().filter(Objects::nonNull).map(KeyValue::new);
            RDD<KeyValue> rdd = rows.sortBy(new org.apache.spark.api.java.function.Function<KeyValue, Integer>() {
                @Override
                public Integer call(KeyValue v1) throws Exception {
                    return v1.getValue();
                }
            }, false, 4).filter(Objects::nonNull).rdd();
            System.out.println("----------rdd---------" + rdd.count());
        } finally {
            if (session != null) {
                session.close();
            }
        }
    }

    /**
     * Caused by: java.lang.ClassCastException: com.github.yafeiwang1240.sparkoperator.problem.Filter$KeyValue cannot be cast to org.apache.spark.sql.Row
     */
    protected void functionGenerics() {
        SparkSession session = null;
        try {
            session = SparkSession.builder().appName("filter")
                    .enableHiveSupport().getOrCreate();
            org.apache.spark.sql.Dataset<Row> ds = session.sql("select c_name as name, c_workyear as value from user_c limit 100");
            JavaRDD<KeyValue> rows = ds.toJavaRDD().filter(Filter::nonNull).map(KeyValue::new);
            RDD<KeyValue> rdd = rows.filter(Filter::nonNull).rdd();
            System.out.println("----------rdd---------" + rdd.count());
        } finally {
            if (session != null) {
                session.close();
            }
        }
    }

    /**
     * 自定义lambda Ok
     */
    protected void functionLambda() {
        SparkSession session = null;
        try {
            session = SparkSession.builder().appName("filter")
                    .enableHiveSupport().getOrCreate();
            org.apache.spark.sql.Dataset<Row> ds = session.sql("select c_name as name, c_workyear as value from user_c limit 100");
            JavaRDD<KeyValue> rows = ds.toJavaRDD().filter(v -> v != null).map(KeyValue::new);
            RDD<KeyValue> rdd = rows.filter(v -> v != null).rdd();
            System.out.println("----------rdd---------" + rdd.count());
        } finally {
            if (session != null) {
                session.close();
            }
        }
    }

    /**
     * 不同的函数是ok的
     */
    protected void functionAnother() {
        SparkSession session = null;
        try {
            session = SparkSession.builder().appName("filter")
                    .enableHiveSupport().getOrCreate();
            org.apache.spark.sql.Dataset<Row> ds = session.sql("select c_name as name, c_workyear as value from user_c limit 100");
            JavaRDD<KeyValue> rows = ds.toJavaRDD().filter(Objects::nonNull).map(KeyValue::new);
            RDD<KeyValue> rdd = rows.filter(v -> v != null).rdd();
            System.out.println("----------rdd---------" + rdd.count());
        } finally {
            if (session != null) {
                session.close();
            }
        }
    }

    /**
     * 使用匿名内部类是ok的
     */
    protected void functionAnon() {
        SparkSession session = null;
        try {
            session = SparkSession.builder().appName("filter")
                    .enableHiveSupport().getOrCreate();
            org.apache.spark.sql.Dataset<Row> ds = session.sql("select c_name as name, c_workyear as value from user_c limit 100");
            JavaRDD<KeyValue> rows = ds.toJavaRDD().filter(new org.apache.spark.api.java.function.Function<Row, Boolean>() {
                @Override
                public Boolean call(Row v1) throws Exception {
                    return v1 != null;
                }
            }).map(KeyValue::new);
            RDD<KeyValue> rdd = rows.filter(new org.apache.spark.api.java.function.Function<KeyValue, Boolean>() {
                @Override
                public Boolean call(KeyValue v1) throws Exception {
                    return v1 != null;
                }
            }).rdd();
            System.out.println("----------rdd---------" + rdd.count());
        } finally {
            if (session != null) {
                session.close();
            }
        }
    }

    /**
     * 自定义泛型类也是ok的
     */
    protected void functionGenericClass() {
        SparkSession session = null;
        try {
            session = SparkSession.builder().appName("filter")
                    .enableHiveSupport().getOrCreate();
            org.apache.spark.sql.Dataset<Row> ds = session.sql("select c_name as name, c_workyear as value from user_c limit 100");
            JavaRDD<KeyValue> rows = ds.toJavaRDD().filter(new DoFilter<>()).map(KeyValue::new);
            RDD<KeyValue> rdd = rows.filter(new DoFilter<>()).rdd();
            System.out.println("----------rdd---------" + rdd.count());
        } finally {
            if (session != null) {
                session.close();
            }
        }
    }

    /**
     * lambda同一函数也是ok
     */
    protected void functionLambdaFunction() {
        SparkSession session = null;
        try {
            session = SparkSession.builder().appName("filter")
                    .enableHiveSupport().getOrCreate();
            org.apache.spark.sql.Dataset<Row> ds = session.sql("select c_name as name, c_workyear as value from user_c limit 100");
            JavaRDD<KeyValue> rows = ds.toJavaRDD().filter(v1 -> nonNull(v1)).map(KeyValue::new);
            RDD<KeyValue> rdd = rows.filter(v1 -> nonNull(v1)).rdd();
            System.out.println("----------rdd---------" + rdd.count());
        } finally {
            if (session != null) {
                session.close();
            }
        }
    }

    public static <T> boolean nonNull(T t) {
        return t != null;
    }

    public static class DoFilter<T> implements org.apache.spark.api.java.function.Function<T, Boolean> {

        @Override
        public Boolean call(T v1) throws Exception {
            return v1 != null;
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
