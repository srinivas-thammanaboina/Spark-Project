package examples

import org.apache.spark.SparkContext

import org.apache.log4j._

object auctionDFApp extends App{
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sc = new SparkContext("local[*]","Auction Data frame app")
  
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  
  import sqlContext.implicits._
  
  case class Auction(aucid:String, bid:Float, bidTime:Float, bidder:String, bidrate:Int, openbid:Float, 
                      price:Float,itemType:String,dtl:Int)
  
  val inputFile = sc.textFile("data/auctiondata.csv").map(_.split(","))
  
  val auctionRdd = inputFile.map(x => Auction(x(0),x(1).toFloat,x(2).toFloat,x(3),x(4).toInt,x(5).toFloat,x(6).toFloat,x(7),x(8).toInt))
  
  val auctionDF = auctionRdd.toDF()
  
  auctionDF.registerTempTable("auctionDF")
  
  println("==== show is to print top 20 rows===")
  
  auctionDF.show()
  
  println("======== printint schema =======")
  
  auctionDF.printSchema()
  
  println("===== total num of bids ========")
  
  println(auctionDF.count())
  
  println("====== total num of distinct auctions =========")
  
  println(auctionDF.select("aucid").distinct().count())
  
  println("====== total num of distinct item types =========")
  
  println(auctionDF.select("itemType").distinct().count())
  
  println("====== total num of bids per auction and item type =========")
  
  auctionDF.groupBy($"aucid",$"itemType").count.show()
  
  //println(auctionDF.groupBy($"aucid",$"itemType").count)
  
  println("======= For each auction item and item type max, min and average number of bids =====")
  
  //auctionDF.groupBy($"aucid",$"itemType").count.agg(min("count"), avg("count"), max("count")).show

  println("========== number of auctions with final price greater than 200 ========")
  
  println(auctionDF.filter($"price">200).count)
  
  println("======= some basic statistics on all auctions that are of type xbox =======")
  
  val xboxes = sqlContext.sql("SELECT aucid,itemType,bid,openbid,price from auctionDF where itemType='xbox'")
  
  xboxes.describe("price").show
  
  xboxes.describe("bid").show
  
  xboxes.describe("openbid").show
}