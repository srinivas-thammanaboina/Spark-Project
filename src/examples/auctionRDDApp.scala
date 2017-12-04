package examples

import org.apache.log4j._
import org.apache.spark.SparkContext


/**
 * auctiondata.csv consists of online auction data
 * 
 * schema: auction id, bid amount, bid time from start of auction, bidder's user id, bider's rating, opening price, final price, item type, days to live
 */
object auctionRDDApp extends App{
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
   
  val sc = new SparkContext("local[*]","Auction biddings")
  
  val auctionRdd = sc.textFile("data/auctiondata.csv").map(_.split(","))
  
  println("==== first element of auctionRdd ========")
  
  auctionRdd.first.foreach(println)
  
  println("====== 5th element of auctionRdd =======")
  
  auctionRdd.take(5).last.foreach(println)
  
  println("3 ====== total number of bids ======")
  
  println(auctionRdd.count())
  
  println("4 ==== distinct number of items that are auctioned =====")
  
  println(auctionRdd.map(x=>x(0)).distinct().count())
  
  println("5 ====== total number of item types that were auctioned? ==========")

  println(auctionRdd.map(x=>x(7)).distinct().count())
  
  println("===========> calculate the max, min and average number of bids among all the auctioned items <=========")
  
  println("==== total number of bids per auction ======")
  
  val bids_per_auction = auctionRdd.map(x=> (x(0),1)).reduceByKey(_+_)
  
  println(bids_per_auction.foreach(println))
  
  println("===== across all actioned items what is the max number of bids =======")
  
  import java.lang.Math
  
  println(bids_per_auction.map(x => x._2).reduce((x,y)=> Math.max(x, y)))
  
  println("===== across all actioned items what is the min number of bids =======")
  
  println(bids_per_auction.map(x => x._2).reduce((x,y)=> Math.min(x, y)))
  
  
}