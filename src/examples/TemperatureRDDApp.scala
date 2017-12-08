package examples

import org.apache.spark.sql.SparkSession

import org.apache.log4j._

object TemperatureRDDApp {
  
  def parseLine(line:String) = {
    val fields = line.split(",")
    val stationId = fields(0)
    val entryType = fields(2)
    val temperature = fields(3).toFloat*0.1f*(9.0f/5.0f)+32.0f
    (stationId,entryType,temperature)     
  }
  def main(args: Array[String]): Unit = {
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val spark = SparkSession.builder().master("local").appName("filter and min temperature using rdd").getOrCreate()
    
    val sc = spark.sparkContext
    
    val inputrdd = sc.textFile("data/1800.csv").map(parseLine)
    
    val filterMinTemp = inputrdd.filter(x => x._2=="TMIN")
    
    import java.lang.Math._
    
    val minTempBystationId = filterMinTemp.map(x=>(x._1,x._3.toFloat)).reduceByKey((x,y)=> min(x,y))
    
    val results = minTempBystationId.collect()
    
    for(record <- results.sorted){
      val station = record._1
      val mintemp = record._2
      val formattedTemp = f"$mintemp%.2f F"
      println(s"$station minimum temperature: $formattedTemp")
    }
  }
}