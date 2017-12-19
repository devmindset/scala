package com.scala.deb

import org.apache.spark.sql
import org.apache.spark
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Transpose {
  
  val conf = new SparkConf().setMaster("local[*]")
  val ss = SparkSession.builder().appName("Transpose").config(conf).getOrCreate()
  case class Number(uid:Int, col1:Int, col2:Int, col3:Int, col4:Int, col5:Int, col6:Int)
  
  def main(args:Array[String]){
    
    val df = ss.createDataFrame(Seq(
  (1, 1, 2, 3, 8, 4, 5),
  (2, 4, 3, 8, 7, 9, 8),
  (3, 6, 1, 9, 2, 3, 6),
  (4, 7, 8, 6, 9, 4, 5),
  (5, 9, 2, 7, 8, 7, 3),
  (6, 1, 1, 4, 2, 8, 4)
)).toDF("uid", "col1", "col2", "col3", "col4", "col5", "col6")

 df.show(10,false)
 //Required for sum operation
 import org.apache.spark.sql.functions._
 val df_result= df.groupBy().agg(sum("uid") as "UID", sum("col1") as "col1", sum("col2") as "col2"
     , sum("col3") as "col3", sum("col4") as "col4", sum("col5") as "col5", sum("col6") as "col6")
 df_result.show(2,false) 
 
/* val arr_col =df_result.columns
 arr_col.foreach { x => println(x) }
 
  var arr_col_val = Array[String]()  
  //df_result.foreach { x => println(x(0)) }
  df_result.foreach { x =>  {
                             arr_col_val = x.mkString(",").split(",")
                             } 
  arr_col_val.foreach { x => println(x) }                 }
  val intArr = arr_col_val.map { x => x.toInt }
  intArr.foreach { x => println(x) }*/
  
 // val df_transpose = ss.createDataFrame(arr_col zip intArr).toDF("Tra1","Tra2")
//  df_transpose.show(10,false)  
  
/*  import ss.sqlContext.implicits._
  val dd =ss.sparkContext.parallelize(arr_col zip intArr).toDF("Tra1","Tra2")
  dd.show()*/
 val list_col =df_result.columns.toList
 list_col.foreach(println)
 
 val list_col_val = df_result.first().toSeq.asInstanceOf[Seq[Long]]
 list_col_val.foreach (println)

 import ss.sqlContext.implicits._
  val dd =ss.sparkContext.parallelize(list_col zip list_col_val).toDF("Tra1","Tra2")
  dd.show()
 
  df_result.rdd.map(_.toSeq.toList).foreach (println)
/*  df_result.first().toSeq.map{ 
    case x: String => x.toLong
    case x: Long => x 
  }.toL*/
  
  
  }
}

//https://stackoverflow.com/questions/36215755/transpose-dataframe-using-spark-scala-without-using-pivot-function
//https://hadoopist.wordpress.com/2016/05/28/how-to-transpose-columns-to-rows-in-spark-dataframe/