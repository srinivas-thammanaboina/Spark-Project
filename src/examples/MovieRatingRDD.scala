package examples

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

object MovieRatingRDD extends App{
  
  // Set the log level to only print errors
 Logger.getLogger("org").setLevel(Level.ERROR)
 
 // Create a SparkContext using every core of the local machine, named RatingsCounter
 val sc = new SparkContext("local[*]","Movie Rating counter")
 
 // Load up each line of the ratings data into an RDD
 val lines = sc.textFile("data/ml-100k/u.data")
 
 
 // Convert each line to a string, split it out by tabs, and extract the third field.
    // (The file format is userID, movieID, rating, timestamp)
 val ratings = lines.map(x=> x.toString().split("\t")(2))
 
 ratings.collect().take(30).foreach(println)

 
 // Count up how many times each value (rating) occurs
 val results = ratings.countByValue()
 
  
 results.take(100).foreach(x=>println(x._2))
 
 // Sort the resulting map of (rating, count) tuples
 val sortedResults = results.toSeq.sortBy(_._1)
 
 // Print each result on its own line.
 sortedResults.foreach(println)
 
}