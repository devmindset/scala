package com.scala.deb

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent._
import scala.util._

/*
 The Promise and Future are complementary concepts. The Future is a value which will be retrieved, well,
  sometime in the future and you can do stuff with it when that event happens. It is, therefore, the read 
  or out endpoint of a computation - it is something that you retrieve a value from.

A Promise is, by analogy, the writing side of the computation. You create a promise which is the place where
 you'll put the result of the computation and from that promise you get a future that will be used to read
  the result that was put into the promise. When you'll complete a Promise, either by failure or success, 
  you will trigger all the behavior which was attached to the associated Future. 
 
 */



object PromiseExample {
  def main(args: Array[String]): Unit = {
    val f: Future[Int] = Future(0)
    val p = Promise[Unit]()
    p.future.onSuccess { case _ =>
      println("The program waited patiently for this callback to finish.")
    }
    f.onSuccess { case _ =>
      Thread.sleep(10000)
      p.success(())
    }

    Await.ready(f, Duration.Inf)
    println("F is COMPLETED")
    Await.ready(p.future, Duration.Inf)
    println("P is COMPLETED")
  }
}