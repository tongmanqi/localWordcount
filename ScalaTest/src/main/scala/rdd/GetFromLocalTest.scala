package rdd

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object GetFromLocalTest {
  def main(args: Array[String]): Unit = {
    // 屏蔽不必要的日志显示在终端上
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val conf = new SparkConf().setMaster("local").setAppName("wc_local")	//创建环境变量
    val sc = new SparkContext(conf)								//创建环境变量实例
    val textFile  = sc.textFile("src/main/resources/urldata.txt")
    val result = textFile.flatMap(_.split(" "))
      .map(word => (word, 1)).reduceByKey(_ + _)
    result.saveAsTextFile("src/main/resources/wc_local")

    sc.stop()
  }
}
