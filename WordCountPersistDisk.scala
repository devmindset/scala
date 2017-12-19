package com.spark.traing2

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel

object WordCountwithPersistDisk {
  
  def main(args: Array[String]) {
    
     println(" ############# < Word Count With Persist Disk > ############ ");

    val conf = new SparkConf().setMaster("local").setAppName("RDDJoins");
    val sc = new SparkContext(conf);

    val lines = sc.textFile("D:\\selfstudy\\data\\sample.txt", 3);

    val words = lines.flatMap(x => x.split(" "));
    val units = words.map(word => (word, 1)).persist(StorageLevel.MEMORY_ONLY);
    val counts = units.reduceByKey((x, y) => x + y)

    println("counts.toDebugString : " + counts.toDebugString);
    
/*res1: String = 
(3) ShuffledRDD[4] at reduceByKey at <console>:34 []
 +-(3) MapPartitionsRDD[3] at map at <console>:32 []
    |  MapPartitionsRDD[2] at flatMap at <console>:30 []
    |  hdfs:///user/raj/data.txt MapPartitionsRDD[1] at textFile at <console>:28 []
    |  hdfs:///user/raj/data.txt HadoopRDD[0] at textFile at <console>:28 [] */
     
    
    // First Action
    counts.collect().foreach(f => println(f));
    val counts2 = units.reduceByKey((x, y) => x + y)

    println("counts2.toDebugString : " + counts2.toDebugString);
    // Second Action
    
/*    res3: String = 
(3) ShuffledRDD[5] at reduceByKey at <console>:34 []
 +-(3) MapPartitionsRDD[3] at map at <console>:32 []
    |      CachedPartitions: 3; MemorySize: 696.0 B; ExternalBlockStoreSize: 0.0 B; DiskSize: 0.0 B
    |  MapPartitionsRDD[2] at flatMap at <console>:30 []
    |  hdfs:///user/raj/data.txt MapPartitionsRDD[1] at textFile at <console>:28 []
    |  hdfs:///user/raj/data.txt HadoopRDD[0] at textFile at <console>:28 [] */

    counts2.collect().foreach(f => println(f));

  }
  
}