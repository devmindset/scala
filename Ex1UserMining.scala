/*package com.spark.traing2


import org.apache.spark.{SparkContext, SparkConf}

import org.apache.spark.rdd._
import com.duchessfr.spark.utils.TweetUtils
import com.duchessfr.spark.utils.TweetUtils._

*//**
 *  The scala API documentation: http://spark.apache.org/docs/latest/api/scala/index.html
 *
 *  We still use the dataset with the 8198 reduced tweets. The data are reduced tweets as the example below:
 *
 *  {"id":"572692378957430785",
 *  "user":"Srkian_nishu :)",
 *  "text":"@always_nidhi @YouTube no i dnt understand bt i loved of this mve is rocking",
 *  "place":"Orissa",
 *  "country":"India"}
 *
 *  We want to make some computations on the users:
 *  - find all the tweets by user
 *  - find how many tweets each user has
 *
 *  Use the Ex1UserMiningSpec to implement the code.
 *//*
object Ex1UserMining {

  val pathToFile = "D:\\Books\\BigData\\SparkWorkshop\\Training\\SparkExampleProject\\spark-in-practice-scala-master\\data\\reduced-tweets.json"

  *//**
   *  Load the data from the json file and return an RDD of Tweet
   *//*
  def loadData(): RDD[Tweet] = {
    // Create the spark configuration and spark context
    val conf = new SparkConf()
        .setAppName("User mining")
        .setMaster("local[*]")

    val sc = new SparkContext(conf)

    // Load the data and parse it into a Tweet.
    // Look at the Tweet Object in the TweetUtils class.
    sc.textFile(pathToFile).mapPartitions(TweetUtils.parseFromJson(_))
  }
  
  def main(args: Array[String]) {
    
    val topTen = topTenTwitterers()
    
    println("NumberOfUser")
   
  } 

  *//**
   *   For each user return all his tweets
   *//*
  def tweetsByUser(): RDD[(String, Iterable[Tweet])] = {
    val tweets = loadData
    // TODO write code here
    // Hint: the Spark API provides a groupBy method
    val tweetPair = tweets.map { x => (x.user, x) }
    val userwiseTweets = tweetPair.groupByKey()
    println("Evaluating group");
    println("DebugString : " + userwiseTweets.toDebugString);
    userwiseTweets
  }

  *//**
   *  Compute the number of tweets by user
   *//*
  def tweetByUserNumber(): RDD[(String, Int)] = {
    val tweets = loadData

    // TODO write code here
    // Hint: think about what you did in the wordcount example
    
    val tweetPair = tweets.map { x => (x.user, 1) }
    val noOfTweetsPerUser = tweetPair.reduceByKey((x,y) => x + y)
    println("Evaluating group");
    println("DebugString : " + noOfTweetsPerUser.toDebugString);
    noOfTweetsPerUser
  }


  *//**
   *  Top 10 twitterers
   *//*
  def topTenTwitterers(): Array[(String, Int)] = {
    val tweets = loadData
    implicit val sortArray = new Ordering[(String,Int)] {
    override def compare(a: (String,Int), b: (String,Int)) = a._2.compareTo(b._2)
    }
    
    val tweetPair = tweets.map { x => (x.user, 1) }
    val noOfTweetsPerUser = tweetPair.reduceByKey((x,y) => x + y)
    
    val topTen = noOfTweetsPerUser.top(10)
    
    topTen.foreach(x=> println(x._1+"="+ x._2))
    topTen
    
  }
  
}
*/
