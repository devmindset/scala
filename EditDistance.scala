package com.scala.deb

/*
 * 
 * "ACT" to meaningful word "CAT" algorithm ? 
 *	  using spark or scala (edit distance)
 */

object EditDistance {
  
   def minimum(i1: Int, i2: Int, i3: Int)=math.min(math.min(i1, i2), i3)
  
  def distance(orgStr:String,newStr:String):Int = {
   //Approach1 
    if(orgStr.length() == 0 ) 
      newStr.length()
    else if(newStr.length() == 0 ) 
      orgStr.length()
    else 
      0
    
   val matrix =  Array.ofDim[Int](orgStr.length()+1,newStr.length()+1)
   for (i <- 0 to orgStr.length()) {
         for ( j <- 0 to newStr.length()) {
                 if(i==0)
                 matrix(i)(j) = j
                 else if(j==0)
                 matrix(i)(j) = i
                 else 
                 matrix(i)(j) = 0  
            print(" " + matrix(i)(j));
         }
         println();
    }
    
    //Approach 2
    /*val matrix = Array.tabulate(orgStr.length()+1, newStr.length()+1){
        (str1Len,str2Len) => (if(str1Len == 0 ) str2Len
                              else if(str2Len == 0 ) str1Len
                              else 0                        
                              )
    }
    for (i <- 0 until orgStr.length()) {
         for ( j <- 0 to newStr.length()) {
            print(" " + matrix(i)(j));
         }
         println();
    }*/
    /*
 0 1 2 3 4 5 6 7
 1 0 0 0 0 0 0 0
 2 0 0 0 0 0 0 0
 3 0 0 0 0 0 0 0
 4 0 0 0 0 0 0 0
 5 0 0 0 0 0 0 0
 6 0 0 0 0 0 0 0
     */
    
    //https://rosettacode.org/wiki/Levenshtein_distance#Scala
    
    
    for (i <- 0 until orgStr.length()) {
         for ( j <- 0 until newStr.length()) {
            if(orgStr(i) == newStr(j)){
              matrix(i+1)(j+1) = matrix(i)(j)
            } else {
              matrix(i+1)(j+1) = minimum(matrix(i)(j),matrix(i+1)(j),matrix(i)(j+1))+1
            }
         }
    }
    
    for (i <- 0 to orgStr.length()) {
         for ( j <- 0 to newStr.length()) {
            print(" " + matrix(i)(j));
         }
         println();
    }
    
    return matrix(orgStr.length())(newStr.length());
  }
  
  def printDistance(orgStr:String,newStr:String) = {
    println("%s -> %s : %d".format(orgStr,newStr,distance(orgStr,newStr)))
  }
  
  def main(args:Array[String]){

 // printDistance("kitten", "sitting")  
     printDistance("ACT", "CAT")
  }
}