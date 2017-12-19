/**
 * reduceByKey, groupByKey and combineByKey
 */
package rdd_demo

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object aggregateAll {
  
  var inputPath = "D:\\Books\\BigData\\SparkWorkshop\\Training\\Data\\input_data\\"
  var outputPath = "D:\\Books\\BigData\\SparkWorkshop\\Training\\Data\\output_data\\"

  val conf = new SparkConf().setMaster("local")
  val sc = SparkSession
    .builder
    .appName("Aggregate-All")
    .config(conf)
    .getOrCreate().sparkContext

  def main(args: Array[String]): Unit = {

    val mydata = sc.textFile(inputPath + "soccer.txt")
    
    //Converting data to a tuple, by splitting at delimiter. Score converted to a number explicitly
    val myPair = mydata.map { k => (k.split(" ")(0), k.split(" ")(1).toInt) }
    
    // Now let us try groupByKey to get sum of the goals in last 4 years for players
    myPair.groupByKey().foreach(println)
    
    myPair.groupByKey().mapValues { x => x.reduce((a, b) => a + b) }.foreach(println)
    
    println("Another method to do same thing.")
    myPair.groupByKey().map { x => (x._1, x._2.sum) }.foreach(println)
    
    
    println("reduceByKey")
    myPair.reduceByKey { case (a, b) => a + b }.foreach { println }
    
    
    println("combineByKey")
    myPair.combineByKey(
      (comb: Int) => {
        println(s"""createCombiner is going to create first combiner for ${comb}""")
        (comb)
      }, (a: Int, comb: Int) => {
        println(s"""mergeValue is going to merge ${a}+${comb} values in a single partition""")
        (a + comb)
      }, (a: Int, b: Int) => {
        println(s"""mergeCombiner is going to merge ${a}+${b} combiners across partition""")
        (a + b)
      }).foreach(println)

    
    // AggregateByKey Summing
    val keysWithValuesList = Array("foo=A", "foo=A", "foo=A", "foo=A", "foo=B", "bar=C", "bar=D", "bar=D")
    val data = sc.parallelize(keysWithValuesList)
    //Create key value pairs
    val kv = data.map(_.split("=")).map(v => (v(0), v(1))).cache()

    val initialCount = 0;
    val addToCounts = (n: Int, v: String) => n + 1
    val sumPartitionCounts = (p1: Int, p2: Int) => p1 + p2

    val countByKey = kv.aggregateByKey(initialCount)(addToCounts, sumPartitionCounts)
    
    Thread.sleep(1000000)
    
  }

}