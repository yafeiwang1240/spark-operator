package com.github.yafeiwang1240.sparkoperator.problem;

import com.github.yafeiwang1240.Function;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * 显式的退出
 * <cluster>
 *     <cmd>
 *         spark-submit --master yarn --deploy-mode cluster --class com.github.yafeiwang1240.Job spark-operator-1.0-SNAPSHOT.jar
 *     </cmd>
 *     <exitCode>-1</exitCode>
 * </cluster>
 *
 * <local>
 *     <cmd>
 *         spark-submit --master local[2] --class com.github.yafeiwang1240.Job spark-operator-1.0-SNAPSHOT.jar
 *     </cmd>
 *     <exitCode>0</exitCode>
 * </local>
 *
 * <client>
 *     <cmd>
 *         spark-submit --master yarn --deploy-mode client --class com.github.yafeiwang1240.Job spark-operator-1.0-SNAPSHOT.jar
 *     </cmd>
 *     <exitCode>0</exitCode>
 * </client>
 * @author wangyafei
 */
public class SystemExit implements Function {


    @Override
    public void function() {
        SparkSession session = null;
        try {
            session = SparkSession.builder().appName("SystemExit")
                    .enableHiveSupport().getOrCreate();
            org.apache.spark.sql.Dataset<Row> ds = session.sql("select c_name as name, c_workyear as value from user_c limit 100");
            ds.toJavaRDD().foreach(v -> {
                System.out.println(v);
            });
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        } finally {
            if (session != null) {
                session.close();
            }
        }
        /**{@link org.apache.spark.deploy.yarn.ApplicationMaster#runImpl()}*/
        // The default state of ApplicationMaster is failed if it is invoked by shut down hook.
        // This behavior is different compared to 1.x version.
        // If user application is exited ahead of time by calling System.exit(N), here mark
        // this application as failed with EXIT_EARLY. For a good shutdown, user shouldn't call
        // System.exit(0) to terminate the application.
        System.exit(0);
    }
}
