package com.github.yafeiwang1240

import org.apache.spark.sql.SparkSession

object WordCount {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("WordCount").getOrCreate()
    val rdd = spark.sparkContext.objectFile("/tmp/wordcount/")
    val pair = rdd.map(x => (x, 1))
    val reduce = pair.reduceByKey((x, y) => x + y)
    reduce.collect().foreach((x) => println(x._1 + ": " + x._2))
    spark.close()
  }

}
