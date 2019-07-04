package rdd

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Level, Logger}

object WordCountTest {
  def main(args: Array[String]) {
    // 屏蔽不必要的日志显示在终端上
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val conf = new SparkConf().setMaster("local").setAppName("wc")	//创建环境变量
    val sc = new SparkContext(conf)								//创建环境变量实例
    val data = sc.textFile("src/main/resources/urldata.txt")								//读取文件
    data.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_+_).collect().foreach(println)	//word计数

    sc.stop()
  }
}

