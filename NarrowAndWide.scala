package com.spark.traing2

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.rddToOrderedRDDFunctions
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.sql.SparkSession


object NarrowAndWide {

  val conf = new SparkConf().setMaster("local")
  val sc = SparkSession
    .builder
    .appName("Narrow And Wide")
    .config(conf)
    .getOrCreate().sparkContext

  def main(args: Array[String]): Unit = {
    
   val rdd1 = sc.parallelize(List("BigData",
                                 "CoreJava",
                                 "BigData",
                                 "Testing",
                                 "Testing",
                                 "BigData"))
                                
    //Narrow dependency. Map the rdd to tuples  of (x, 1)
    val rdd3 = rdd1.map(x => (x, 1))
    //wide dependency groupByKey
    val rdd4 = rdd3.groupByKey()
    
    rdd4.count()
    
    Thread.sleep(1000000)
    
  }

}
