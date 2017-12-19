package rdd_demo

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object PairRDD {

  val conf = new SparkConf().setMaster("local")
  val sc = SparkSession
    .builder
    .appName("Pair RDD")
    .config(conf)
    .getOrCreate().sparkContext

  def main(args: Array[String]) {

    val inputrdd = sc.parallelize(List("key1 101", "key3 303", "key3 505", "key1 202"));

    val pairRdd = inputrdd.map { x =>
      val splitted = x.split(" ")
      (splitted(0), splitted(1).toInt)
    }
    val reduced = pairRdd.reduceByKey((x, y) => x + y)

    reduced.foreach(f => println(f));
    println("DebugString : " + reduced.toDebugString);
    Thread.sleep(1000000)
  }
}