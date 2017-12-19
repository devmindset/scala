package rdd_demo

import scala.util.Random

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object MutableRDD {

  val conf = new SparkConf().setMaster("local")
  val sc = SparkSession
    .builder
    .appName("Mutable RDD")
    .config(conf)
    .getOrCreate().sparkContext

  def main(args: Array[String]) {

    // start with a sequence of 10,000 zeros
    val zeros = Seq.fill(10000)(0)

    // create a RDD from the sequence, and replace all zeros with random values
    val randomRDD = sc.parallelize(zeros).map(x => Random.nextInt())

    // filter out all non-positive values, roughly half the set
    val filteredRDD = randomRDD.filter(x => x > 0)

    // count the number of elements that remain, twice
    val count1 = filteredRDD.count()
    val count2 = filteredRDD.count()

    // Since filteredRDD is immutable, this should always pass, right? 
    //assert(count1 == count2, "\nMismatch!  count1=+count1+ count2=+count2")
    System.out.println("Is both values are same", count1 == count2)

    System.out.println("Program completed successfully")
    
    Thread.sleep(100000)
  }
}