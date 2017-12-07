package examples

import org.apache.log4j._
import org.apache.spark.sql.SparkSession

object AvgNumOfFriendsByAge {
  
  
  def main(args: Array[String]): Unit = {
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    
        
    val spark = SparkSession.builder().master("local").appName("Average no of friends by age on social network").getOrCreate()
    
    val sc = spark.sparkContext
    
    val inputFile = sc.textFile("data/fakefriends.csv").map(_.split(","))
    
    val totalbyAgeFriends = inputFile.map(x => (x(2).toInt,x(3).toInt)).mapValues(x => (x,1))
    
                            .reduceByKey((x,y)=>(x._1+y._1,x._2+y._2))
                            
    /**
     * mapValues() = > (33,385) => (33,(385,1))
     * 								 (33,38) => (33,(38,1))
     * 								 (23,45) => (23,(45,1))
     * 									(23,55) => (23,(55,1))
     * 
     * reduceByKey -> (33,(385+38,1+1)
     * 								 (23,(100,2))
     */
    
    val avgFriendsByAge = totalbyAgeFriends.mapValues(x=> (x._1/x._2))
    
    avgFriendsByAge.foreach(println)
  }
}