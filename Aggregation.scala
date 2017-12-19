package com.spark.traing2

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

object Aggregation {
  
	def main(args:Array[String]): Unit ={
	  
	  val conf = new SparkConf().setMaster("local").setAppName("Aggregation");
    val sc = new SparkContext(conf);

			val z = sc.parallelize(List(1,2,3,4,5,6), 2)

					// lets first print out the contents of the RDD with partition labels
					def myfunc(index: Int, iter: Iterator[(Int)]) : Iterator[String] = {
							iter.toList.map(x => "[partID:" +  index + ", val: " + x + "]").iterator
			}

			val z1 = z.mapPartitionsWithIndex(myfunc).collect
			//res28: Array[String] = Array([partID:0, val: 1], [partID:0, val: 2], [partID:0, val: 3], [partID:1, val: 4], [partID:1, val: 5], [partID:1, val: 6])
			println("z1="+z)
			z1.foreach { x => println(x.toString()) }
			
			val z2 = z.aggregate(0)(math.max(_, _), _ + _)
			//res40: Int = 9
			println("z2="+z2)
			//z2.foreach { x => println(x.toString()) }

			// This example returns 16 since the initial value is 5
			// reduce of partition 0 will be max(5, 1, 2, 3) = 5
			// reduce of partition 1 will be max(5, 4, 5, 6) = 6
			// final reduce across partitions will be 5 + 5 + 6 = 16
			// note the final reduce include the initial value
			val z3 = z.aggregate(5)(math.max(_, _), _ + _)
			//res29: Int = 16
			println("z3="+z3)
			//z3.foreach { x => println(x.toString()) }


			val z4 = sc.parallelize(List("a","b","c","d","e","f"),2)

			//lets first print out the contents of the RDD with partition labels
			def myfunc1(index: Int, iter: Iterator[(String)]) : Iterator[String] = {
					iter.toList.map(x => "[partID:" +  index + ", val: " + x + "]").iterator
			}

			val z5 = z4.mapPartitionsWithIndex(myfunc1).collect
			//res31: Array[String] = Array([partID:0, val: a], [partID:0, val: b], [partID:0, val: c], [partID:1, val: d], [partID:1, val: e], [partID:1, val: f])
			z5.foreach { x => println(x.toString()) }
			
			val z6 = z4.aggregate("")(_ + _, _+_)
			//res115: String = abcdef
			println("z6="+z6)

			// See here how the initial value "x" is applied three times.
			//  - once for each partition
			//  - once when combining all the partitions in the second reduce function.
			val z7 = z4.aggregate("x")(_ + _, _+_)
			//res116: String = xxdefxabc
			println("z7="+z7)

			// Below are some more advanced examples. Some are quite tricky to work out.

			/*val z8 = sc.parallelize(List("12","23","345","4567"),2)
			z8.aggregate("")((x,y) => math.max(x.length, y.length).toString, (x,y) => x + y)
			//res141: String = 42
			println("z7="+z2)

			z8.aggregate("")((x,y) => math.min(x.length, y.length).toString, (x,y) => x + y)
			//res142: String = 11
			println("z8="+z2)

			val z11 = sc.parallelize(List("12","23","345",""),2)
			z11.aggregate("")((x,y) => math.min(x.length, y.length).toString, (x,y) => x + y)
			//res143: String = 10
			println("z9="+z3)*/
	}
   
}

 
  
