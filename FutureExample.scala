package com.scala.deb

import scala.concurrent.{Await,Future,future}
// Implicits.global imports the “default global execution context.” You can think of an execution context 
//as being a thread pool, and this is a simple way to get access to a thread pool.
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success}
import scala.util.Random

object FutureExample {
  
  def sleep(time: Long) { Thread.sleep(time) }
  
  object Cloud {
    def runAlgorithm(i: Int): Future[Int] = future {
        sleep(Random.nextInt(500))
        val result = i + 10
        println(s"returning result from cloud: $result")
        result
    }
}
  
  
  def main(args:Array[String]){
    
    println("Starting Calculation")
    val f = Future{
      sleep(Random.nextInt(500))
      //You can re turn numer like 42 only also you can write aggregation calculation 1+1
     // "Hello World"
      if (Random.nextInt(500) > 250) throw new Exception("Yikes!") else 42

    }
    println("before complete")
    f.onComplete { 
      case Success(value) => println(s"Got call back Value = $value")
      case Failure(e) => e.printStackTrace
    }
    
   // f.onSuccess(pf) - Dont use it
    f onSuccess {
        case result => println(s"Success: $result")
    }
    f onFailure {
        case t => println(s"Exception: ${t.getMessage}")
    }
    
    println("A ..."); sleep(100)
    println("B ..."); sleep(100)
    println("C ..."); sleep(100)
    println("D ..."); sleep(100)
    println("E ..."); sleep(100)
    println("F ..."); sleep(100)
    sleep(2000)
    
    //----------------------------------------------------
    //you may have methods that return futures. The following 
    //example defines a method named longRunningComputation that returns a Future[Int].
    implicit val baseTime = System.currentTimeMillis

    def longRunningComputation(i: Int): Future[Int] = future {
        sleep(100)
        i + 1
    }

    // this does not block
    longRunningComputation(11).onComplete {
        case Success(result) => println(s"result = $result")
        case Failure(e) => e.printStackTrace
    }

    // important: keep the jvm from shutting down
    sleep(1000)
    
    //------------------------------------------------------
    
    println("starting futures")
    val result1 = Cloud.runAlgorithm(10)
    val result2 = Cloud.runAlgorithm(20)
    val result3 = Cloud.runAlgorithm(30)

    println("before for-comprehension")
    val result = for {
        r1 <- result1
        r2 <- result2
        r3 <- result3
    } yield (r1 + r2 + r3)

    println("before onSuccess")
    result onSuccess {
        case result => println(s"total = $result")
    }

    println("before sleep at the end")
    sleep(2000)  // important: keep the jvm alive
    
    //----------------------------------------------------------------------
    
    val f1: Future[Int] = Future(0)
    val f2: Future[Unit] = f1.map { x =>
      Thread.sleep(10000)
      println("The program waited patiently for this callback to finish.")
    }
    //The Await.result method call declares that it will wait for up to one second for the Future to return. 
    //If the Future doesn’t return within that time, it throws a java.util.concurrent.TimeoutException.
    Await.ready(f2, Duration.Inf)
    
    //------------------------------------------------------------------------
    //how can I wait not only for the future to complete but also for that callback to finish?
    val f3: Future[Int] = Future(0)
    val f4 = f3 andThen {
      case Success(v) =>
        Thread.sleep(10000)
        println("The program waited patiently for this callback to finish.")
      case Failure(e) =>
        println(e)
    }

    Await.ready(f3, Duration.Inf)
    println("F3 is COMPLETED")
    Await.ready(f4, Duration.Inf)
    println("F4 is COMPLETED")
    
    /*
     prints  - 
The program waited patiently for this callback to finish.
F3 is COMPLETED
The program waited patiently for this callback to finish.
F4 is COMPLETED
     */
    
  }
}