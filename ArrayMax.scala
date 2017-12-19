package com.scala.deb
//Scala write a program to find maximum difference between the adjacent elements in an array.
object ArrayMax {
   def main(args:Array[String]): Unit ={
     var max_diff = 0;
      var myList = Array(2, 3, 10, 6, 4, 8, 1)  
      for(i<- 0 to (myList.length-2) ){
             if(i!=0 && (myList(i+1)-myList(i))>max_diff  ){
                max_diff = myList(i+1)-myList(i);
             } else if(i==0){
                max_diff = (myList(i+1)-myList(i));
             }
      }
      println(max_diff);
      
      //Find 3rd Largest element in array
      var nrd= 3;
      var OnestElem = myList(0); 
      var secElem = Int.MinValue;
      var thrElem = Int.MinValue;
      
      for(j <- 1  to myList.length-1){
          if(OnestElem < myList(j)){
             thrElem = secElem;
             secElem = OnestElem;
             OnestElem = myList(j); 
          } else if(secElem < myList(j)){
             thrElem = secElem;
             secElem = myList(j);
          } else if(thrElem < myList(j)){
            thrElem = myList(j);
          }
      }
      
      println("3rd largest Element = "+thrElem+" - "+secElem+" - "+OnestElem)
      
   }
}