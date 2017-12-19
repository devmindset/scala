package com.scala.deb

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
/**
 * How will you iterate a RDD?
 */
object IterateRDD {
  val conf = new SparkConf().setMaster("local[*]")
  val sparkContext = SparkSession.builder().config(conf).appName("IterateRDD").getOrCreate().sparkContext
  def main(args:Array[String]){
      val fileData = sparkContext.textFile("data/NumFileForSort.txt")
     // fileData.collect().foreach(println)
      val parts = fileData.partitions
      fileData.foreachPartition { partition => {
               val array = partition.toArray;
               array.foreach(println)
         } 
      }
      
      //2nd approach
      for(p <- parts){
  val idx = p.index
  val partRDD = fileData.mapPartitionsWithIndex((index: Int, it: Iterator[String]) => if(index == idx) it else Iterator(), true)
  val data = partRDD.collect
  
  // Data contains all values from a single partition in the form of array.
  // Now you can do with the data whatever you want: iterate, save to a file, etc.
}
      
      
      
    }
}