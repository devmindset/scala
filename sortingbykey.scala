package rdd_demo

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD.rddToOrderedRDDFunctions
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.sql.SparkSession

object sortingbykey {

  var inputPath = "file:///home/vinay/git_code/input_data/data-master/retail_db"
  var outputPath = "file:///home/vinay/git_code/output_data"
  
  val conf = new SparkConf().setMaster("local")
  val sc = SparkSession
    .builder
    .appName("Sorting By Key")
    .config(conf)
    .getOrCreate().sparkContext

  def main(args: Array[String]): Unit = {

    val orders = sc.textFile(inputPath + "/orders")

    // orders sorted by status
    orders.
      map(order => {
        val o = order.split(",")
        (o(3), order)
      }).
      sortByKey().
      map(_._2).
      take(100).
      foreach(println)

    // orders sorted by status and date in descending order
    orders.
      map(order => {
        val o = order.split(",")
        ((o(3), o(1)), order)
      }).
      sortByKey(false).
      map(_._2).
      take(100).
      foreach(println)

    // let us get top 5 products in each category from products
    val products = sc.textFile(inputPath + "/products")
    val productsGroupByCategory = products.
      filter(product => product.split(",")(4) != "").
      map(product => {
        val p = product.split(",")
        (p(1).toInt, product)
      }).
      groupByKey

    productsGroupByCategory.
      sortByKey().
      flatMap(rec => {
        rec._2.toList.sortBy(r => -r.split(",")(4).toFloat).take(5)
      }).
      take(100).
      foreach(println)

      Thread.sleep(100000)
  }

}