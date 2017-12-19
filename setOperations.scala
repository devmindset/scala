package rdd_demo

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.sql.SparkSession

object setOperations {
  val conf = new SparkConf().setMaster("local")
  val sc = SparkSession
    .builder
    .appName("Set Operations")
    .config(conf)
    .getOrCreate().sparkContext

  var inputPath = "file:///home/vinay/git_code/input_data/data-master/retail_db"
  var outputPath = "file:///home/vinay/git_code/output_data"

  def main(args: Array[String]): Unit = {

    val orders201312 = sc.textFile(inputPath + "/orders").
      filter(order => order.split(",")(1).contains("2013-12")).
      map(order => (order.split(",")(0).toInt, order.split(",")(1)))

    val orderItems = sc.textFile(inputPath + "/order_items").
      map(rec => (rec.split(",")(1).toInt, rec.split(",")(2).toInt))

    val distinctProducts201312 = orders201312.
      join(orderItems).
      map(order => order._2._2).
      distinct

    val orders201401 = sc.textFile(inputPath + "/orders").
      filter(order => order.split(",")(1).contains("2014-01")).
      map(order => (order.split(",")(0).toInt, order.split(",")(1)))

    val products201312 = orders201312.
      join(orderItems).
      map(order => order._2._2)

    val products201401 = orders201401.
      join(orderItems).
      map(order => order._2._2)

    products201312.union(products201401).count
    products201312.union(products201401).distinct.count

    products201312.intersection(products201401).count
    
    Thread.sleep(100000)
  }
}