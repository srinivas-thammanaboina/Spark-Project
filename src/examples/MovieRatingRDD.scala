package examples

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import org.apache.spark.sql.catalyst.expressions.Ascending
import scala.io.Codec
import java.nio.charset.CodingErrorAction
import scala.io.Source

object MovieRatingRDD {
  
  def loadMovieNames() : Map[Int,String] = {
    
    implicit val codec  = Codec("UTF-8")
    
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)
    
    var movieNames : Map[Int,String] = Map()
    
    val lines = Source.fromFile("data/ml-100k/u.item").getLines()
    
    for(line <- lines){
      var fields = line.split('|')
      if(fields.length > 1){
        movieNames += (fields(0).toInt -> fields(1))
      }
    }
    movieNames
  }
  
  def main(args: Array[String]): Unit = {
     
  // Set the log level to only print errors
 Logger.getLogger("org").setLevel(Level.ERROR)
 
 // Create a SparkContext using every core of the local machine, named RatingsCounter
 val sc = new SparkContext("local[*]","Movie Rating counter")
 
 val nameBroadCast = sc.broadcast(loadMovieNames)
 
// nameBroadCast.value.foreach(println)
 // Load up each line of the ratings data into an RDD
 val lines = sc.textFile("data/ml-100k/u.data")
 
 
 // Convert each line to a string, split it out by tabs, and extract the third field.
    // (The file format is userID, movieID, rating, timestamp)
 val ratingofMovies = lines.map(_.split("\t")).map(x => (x(1),x(2).toInt)).reduceByKey(_+_)
                      .map(x=>(x._2,x._1)).sortByKey(true)
 
 
  val ratingwithNameOfMovie = ratingofMovies.map(x=> (nameBroadCast.value.get(x._2.toInt),x._1)).collect()
  
  ratingwithNameOfMovie.foreach(println)
 
  }
 
}