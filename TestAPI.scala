package com.spark.traing2

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

object TestAPI {
  
  def main(args:Array[String]): Unit ={
   
   println(" ############# < RDD Joins > ############ ");
   val conf = new SparkConf().setMaster("local").setAppName("RDDJoins");
   val sc = new SparkContext(conf);
   // Below are some more advanced examples. Some are quite tricky to work out.

		  /*val z = sc.parallelize(List("12","23","","345"),2)
      val x = z.aggregate("")((x,y) => math.min(x.length, y.length).toString, (x,y) => x + y)
     // res144: String = 11
      
      println("x="+x)

			val z11 = sc.parallelize(List("12","23","345",""),2)
			val y = z11.aggregate("")((x,y) => math.min(x.length, y.length).toString, (x,y) => x + y)
			//res143: String = 10
			println("y="+y)*/
   
      val z = sc.parallelize(List("12".length(),"23".length(),"".length(),"345".length()),2)
      val x = z.aggregate(0)((x,y) => math.min(x, y), (x,y) => x + y)
     // res144: String = 11
      
      println("x="+x)

			val z11 = sc.parallelize(List("12".length(),"23".length(),"345".length(),"".length()),2)
			val y = z11.aggregate(0)((x,y) => math.min(x, y), (x,y) => x + y)
			//res143: String = 10
			println("y="+y)
  }
   
}

 
  
