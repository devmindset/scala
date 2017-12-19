package com.spark.traing2

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object wc {
  
  var inputPath = "file:///home/vinay/git_code/input_data"
  var outputPath = "file:///home/vinay/git_code/output_data"

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local")
    val sc = SparkSession
      .builder
      .appName("Word Count")
      .config(conf)
      .getOrCreate().sparkContext

    val textFile = sc.textFile(inputPath + "/README.md")
    val counts = textFile.flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)

    counts.saveAsTextFile(outputPath + "/output_data/wc")
    
    Thread.sleep(1000000)
  }

}