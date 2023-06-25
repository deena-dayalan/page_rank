package wc

import org.apache.log4j.{Level, LogManager}
import org.apache.spark.HashPartitioner
import org.apache.spark.sql.SparkSession

object PageRank {

  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length < 1) {
      logger.error("Usage:\nwc.PageRankMain <K> <Iteration> <output dir>")
      System.exit(1)
    }
    //Standalone mode//Standalone mode
    /*val spark = SparkSession.builder()
      .appName("Page Rank")
      .master("local[4]")
      .getOrCreate()*/

    //Yarn or AWS mode
    val spark = SparkSession.builder.appName("Page Rank").getOrCreate

    spark.sparkContext.setLogLevel("INFO")

    //Initialize K parameter
    val k = args(0).toInt
    //No. of real pages
    val pages = k*k
    //No. of Iterations
    val iters = args(1).toInt
    //Initial pagerank 1/K^2
    val initPR = 1/pages.toDouble
    //Alpha and Random jump contribution
    val alpha = 0.85.toDouble
    val randJump = (1-alpha)/pages

    //Generate Graph pair RDD
    /*val k_range = List.range(1, (k*k)+2)
    val list = k_range.sliding(2).collect{
      case List(a,b) => if(a % k == 0)(a,0) //dangling page
      else(a,b)//non-dangling page
    }.toList
    val graphRDD = spark.sparkContext.parallelize(list).cache()*/
    
    //Generate Graph pair RDD
    val graphRDD = spark.sparkContext.parallelize(1 to  pages)
      .map({ id => if (id % k == 0)(id, 0) //dangling page
      else (id, id+1) //non-dangling page
      }).cache()
    val partitions = graphRDD.getNumPartitions

    //Ranks for each K^2 real pages and a dummy page '0'.
    //Initialize PageRank=0 for dummy page
    val dummyRDD = spark.sparkContext.parallelize(Seq((0, 0.toDouble)))
    //Initialize PageRank=1/K^2 for real page
    var pageRanks = graphRDD.mapValues{
      f => (initPR) //real page
    }.union(dummyRDD) //pageRanks with real and dummy page ranks
      .partitionBy(new HashPartitioner(partitions)) //maintaining the partition same as graphRDD

    //Run iteration of PageRank
    for (i <- 1 to iters){
      println("############### Iteration: "+i+" ###############")
      //Calculate P(m)/C(m), from the synthetic data; C(m)=1 as only one outgoing edge
      var page_rank = graphRDD.join(pageRanks).map(f => {
        val (pgid,pgrk) = f._2
        (pgid,pgrk)
      }).reduceByKey(_+_)
        .rightOuterJoin(pageRanks).map(f => {
        val (key, value) = f
        value match {
          //Page with incoming links (2,3,5,6,...) PR improves for these pages
          case (Some(prCalc: Double),prInit) => (key,prCalc)
          //Pages with no incoming links (1,4,7,...) exhaust PR=0, i.e only Random jump possible
          case (None,prInit) => (key,0.toDouble)
        }
      }).reduceByKey(_+_)

      //Read out the total dangling PR mass accumulated in dummy page 0,
      var dangSum = page_rank.lookup(0).toList.head

      // Un-cache pageRanks from memory as pageRanks will be updated in next step
      //pageRanks.unpersist()

      //Compute Page Rank for each page
      pageRanks = page_rank.map(f => {
        val (pgId,pgRk) = f
        //Exhaust PR of dummy page as it is re-distributed evenly over all real-pages
        if (pgId == 0)(pgId,0.toDouble)
        //Calculate PR for real pages
        else (pgId, randJump + (alpha * pgRk) + (alpha * (dangSum/pages)))
      })

      //logger.info("Iteration: "+i)
      //logger.info("Sum of PageRanks: "+pageRanks.map(f => f._2).sum())
      //logger.info(pageRanks.toDebugString)
    }
    /*pageRanks.collect().foreach(println)*/
    pageRanks.saveAsTextFile(args(2))
    logger.info("Sum of PageRanks: "+pageRanks.map(f => f._2).sum())
    logger.info(pageRanks.toDebugString)

  }
}
