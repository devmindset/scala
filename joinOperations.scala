package rdd_demo

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.sql.SparkSession

object joinOperations {

  var inputPath = "file:///home/vinay/git_code/input_data/data-master/retail_db"
  var outputPath = "file:///home/vinay/git_code/output_data"

  val conf = new SparkConf().setMaster("local")
  val sc = SparkSession
    .builder
    .appName("Join Operations")
    .config(conf)
    .getOrCreate().sparkContext

  def main(args: Array[String]): Unit = {

    val orders = sc.textFile(inputPath + "/orders").
      map(rec => (rec.split(",")(0).toInt, rec))

    val orderItems = sc.textFile(inputPath + "/order_items").
      map(rec => (rec.split(",")(1).toInt, rec))

    // Join Operation
    val ordersJoin = orders.join(orderItems)
    ordersJoin.take(10).foreach(println)

    // Join LeftOuterJoin
    val ordersLeftOuter = orders.leftOuterJoin(orderItems)
    ordersLeftOuter.filter(rec => rec._2._2 == None).take(10).foreach(println)
    ordersLeftOuter.
      filter(rec => rec._2._2 == None).
      map(rec => rec._2._1).
      take(10).
      foreach(println)

    // When two datasets are already grouped by key and you want
    // to join them and keep them grouped, you can just use cogroup. 
    // That avoids all the overhead associated with unpacking and repacking the groups.  
    // Join Cogroup
    //Avoid the flatMap-join-groupBy pattern
    val ordersCogroup = orders.cogroup(orderItems)
    ordersCogroup.take(10).foreach(println)

    // Cartesian Operation
    val a = sc.parallelize(List(1, 2, 3, 4))
    val b = sc.parallelize(List("Hello", "World"))
    a.cartesian(b).foreach(println)

    Thread.sleep(1000000)
  }
}