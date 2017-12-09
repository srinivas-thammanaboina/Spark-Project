package examples

import org.apache.spark.sql.SparkSession

import org.apache.spark.rdd._
import org.apache.log4j._

object MostPopularSuperHeroRDDApp {
  
  def parseNames(line : String):Option[(Int,String)] = {
    
    val fields = line.split('\"')
  
    if(fields.length>1){
      val id = fields(0).trim().toInt
      val name = fields(1)
      Some(id,name)
    }else{
      None
    }
    
  }
  def parseGraph(line : String) = {
     val fields = line.split("\\s+")
   (fields(0).trim().toInt, fields.length-1)
  }
  def main(args: Array[String]): Unit = {
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val spark = SparkSession.builder().master("local[*]").appName("Most Popular Super Hero").getOrCreate()
    
    val sc = spark.sparkContext
    
    val namesRdd = sc.textFile("data/marvel/Marvel-names.txt").flatMap(parseNames) // flatmap will just discard None results, and extract data from Some results.
    
    val graphRdd = sc.textFile("data/marvel/Marvel-graph.txt").map(parseGraph)
    
    val occurancesPerId = graphRdd.reduceByKey(_+_)
    
    val swap = occurancesPerId.map(x => (x._2,x._1))
    
    val maxoccureneces = swap.max()
    
    val maxOccuredSuperhero = namesRdd.lookup(maxoccureneces._2)(0)
    
    println(maxOccuredSuperhero)
    
  }
}