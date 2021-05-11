package com.github.yafeiwang1240.sparkoperator.monitor;

import org.apache.spark.sql.SparkSession;

/**
 * 取消任务
 * @author wangyafei
 */
public class CancelJobGroup {

    /**
     * 取消任务
     * @param sparkSession
     * @param groupId
     */
    public static void cancelJobGroup(SparkSession sparkSession, String groupId) {
        sparkSession.sparkContext().cancelJobGroup(groupId);
    }
}
