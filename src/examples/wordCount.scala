package examples

import org.apache.spark.sql.SparkSession

import org.apache.log4j._

object wordCount {
  
  def main(args: Array[String]): Unit = {
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val spark = SparkSession.builder().master("local[*]").appName("word count").getOrCreate()
    
    val sc = spark.sparkContext
    
    val inputRdd = sc.textFile("data/book.txt").flatMap(_.split(" "))
    
    val countofWords = inputRdd.countByValue()
    
   // countofWords.foreach(println)
    
    // better way of counting 
    
    val inputRdd2 = sc.textFile("data/book.txt").flatMap(_.split("\\W+"))
    
    val lowerWords = inputRdd.map(_.toLowerCase())
    
    val countofWords2 = lowerWords.countByValue().foreach(println)
  }
}