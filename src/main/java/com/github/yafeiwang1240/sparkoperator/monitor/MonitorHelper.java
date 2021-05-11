package com.github.yafeiwang1240.sparkoperator.monitor;

import com.google.common.collect.Sets;
import org.apache.spark.SparkJobInfo;
import org.apache.spark.SparkStageInfo;
import org.apache.spark.SparkStatusTracker;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.status.api.v1.StageData;
import scala.Option;
import scala.collection.JavaConversions;

import java.util.List;
import java.util.Set;

/**
 * spark job info
 * @author wangyafei
 */
public class MonitorHelper {

    public static void getJobInfo(SparkSession sparkSession, String groupId) {
        SparkStatusTracker tracker = sparkSession.sparkContext().statusTracker();
        int[] jobIds = tracker.getJobIdsForGroup(groupId);
        Set<Integer> stageIds = Sets.newHashSet();
        for (int jobId : jobIds) {
            SparkJobInfo jobInfo = tracker.getJobInfo(jobId).get();
            if (jobInfo == null) continue;
            for (int stageId : jobInfo.stageIds()) {
                stageIds.add(stageId);
                Option<SparkStageInfo> stageInfoOption = tracker.getStageInfo(stageId);
                SparkStageInfo stageInfo = stageInfoOption.get();
                stageInfo.numActiveTasks();
                stageInfo.numFailedTasks();
                stageInfo.numTasks();
                stageInfo.numCompletedTasks();
                List<StageData> stageDataSeq = JavaConversions.<StageData>seqAsJavaList(sparkSession.sparkContext().statusStore().stageData(stageId, true));
                for (StageData stageData : stageDataSeq) {
                    stageData.shuffleWriteBytes();
                    stageData.shuffleReadBytes();
                    stageData.executorCpuTime();
                }
            }
        }
    }
}
