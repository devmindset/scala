package com.scala.deb

object TailRecursive {
  
  /**
   * , note that if you use reversed counting (from bigger one to less one value) you should specify negative step or you will get an empty set:

scala> for(i <- x.length until 0) yield i
res2: scala.collection.immutable.IndexedSeq[Int] = Vector()

scala> for(i <- x.length until 0 by -1) yield i
res3: scala.collection.immutable.IndexedSeq[Int] = Vector(16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1)
   */
  
  def reverse(s: String) : String =
 (for(i <- s.length until 0 by -1) yield s(i-1)).mkString
  
 
      def reverse_tail2(s: String): String = {
         @scala.annotation.tailrec
         def impl(ss: String, r: String): String = {
            if (ss == null) return null
            if (ss.tail.isEmpty) return ss.head + r
            impl(ss.tail, ss.head + r)
          }
          impl(s, "");
      }
 
   def main(args:Array[String]){
     
     val x = "scala is awesome"
     //println(x.reverse)
     
     println(x.tail +" - "+x.head)
     //cala is awesome - s
      //Reverse String using tail  recursion
     println(reverse_tail2(x))
     
     /**
      * Product of list element
      */
      val list = List(1, 2, 3, 4)
        println(product2(List(1, 2, 3, 4)))
        
            // (2) tail-recursive solution
    def product2(ints: List[Int]): Int = {
      @scala.annotation.tailrec
      def productAccumulator(ints: List[Int], accum: Int): Int = {
          ints match {
              case Nil => accum
              case x :: tail => productAccumulator(tail, accum * x)
          }
      }
      productAccumulator(ints, 1)
  }
     
     
     //-----Max element in List 
     @scala.annotation.tailrec
def max(list: List[Int], currentMax: Int = Int.MinValue): Int = {
    if(list.isEmpty) currentMax
    else if ( list.head > currentMax) max(list.tail, list.head)
    else max(list.tail,currentMax)
}
     
   }
}