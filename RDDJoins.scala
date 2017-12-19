package com.spark.traing2

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

object RDDJoins {
  
  def main(args:Array[String]): Unit ={
   
   println(" ############# < RDD Joins > ############ ");
   val conf = new SparkConf().setMaster("local").setAppName("RDDJoins");
   val sc = new SparkContext(conf);
   
   val rdd1 = sc.parallelize(Seq((101, "BigData"),
                                 (102, "CoreJava"),
                                 (103, "BigData"),
                                 (104, "Testing"),
                                 (105, "Testing"),
                                 (106, "BigData")))
                                 
    val rdd2 = sc.parallelize(Seq((101, 40000),
                                  (102, 40000),
                                  (105, 40000),
                                  (108, 40000),
                                  (103, 40000),
                                  (109, 40000)))
     val joined = rdd1.join(rdd2)
     
     joined.foreach(f => println(f));
     println("DebugString : "+joined.toDebugString);
   
/*   
   DebugString : (1) MapPartitionsRDD[4] at join at RDDJoins.scala:28 []
 |  MapPartitionsRDD[3] at join at RDDJoins.scala:28 []
 |  CoGroupedRDD[2] at join at RDDJoins.scala:28 []
 +-(1) ParallelCollectionRDD[0] at parallelize at RDDJoins.scala:15 []
 +-(1) ParallelCollectionRDD[1] at parallelize at RDDJoins.scala:22 []*/
   
  }
   
}

 
  
