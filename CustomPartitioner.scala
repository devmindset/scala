package rdd_demo

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.Partitioner
import org.apache.spark.sql.SparkSession

class MyPartitioner extends Partitioner {
  def numPartitions = 10

  def getPartition(key: Any): Int = {
    key match {
      case s: String => s.length() % numPartitions
    }
  }
}

object CustomPartitioner {

  val conf = new SparkConf().setMaster("local")
  val sc = SparkSession
    .builder
    .appName("Word Count")
    .config(conf)
    .getOrCreate().sparkContext

  def main(args: Array[String]): Unit = {

    val rdd = sc.parallelize(List("word", "stream", "sql", "dataframe", "auction", "averylongword", "anotherveryverylongword"))
    val myPart = new MyPartitioner

    val pairRdd = rdd.map(word => (word, 1))
    val partitionedRdd = pairRdd.partitionBy(myPart)
    partitionedRdd.count()
    
    Thread.sleep(100000)
    
  }

}