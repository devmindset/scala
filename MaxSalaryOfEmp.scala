package com.scala.deb

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object MaxSalaryOfEmp {
  def main(args:Array[String]){
    val conf = new SparkConf().setMaster("local[*]")
    val ss = SparkSession.builder().config(conf).appName("Salary").getOrCreate()
    val sc = ss.sparkContext
    
    val employeeData = sc.parallelize(List((1,"Deb",100),(2,"Ani",300),(3,"Raj",50),(4,"Rup",250)))
    val dummy = (0,"dummy",0)
    //val employeeRDD = sc.makeRDD(employeeData)
    val maxSalariedEmployee = employeeData.fold(dummy)((acc,value) => {if(acc._3 > value._3) acc else value})
    
    println(employeeData.reduce((x,y) => { if(x._3 > y._3) x else y})._3)
    
    val result  = employeeData.reduce((x,y) => { var sum = x._3 + y._3
                                   (1,"Sum=",sum)   
      })
    
    println(result._2+""+result._3)
    
  }
}