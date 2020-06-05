package org.apache.spark.search.sql

import java.io.File
import java.net.URL

import org.apache.lucene.analysis.en.EnglishAnalyzer
import org.apache.spark.search._
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.sys.process._

/**
 * Spark Search Dataset examples.
 */
object SearchDatasetExamples {

  case class Review(asin: String, helpful: Array[Long], overall: Double,
                    reviewText: String, reviewTime: String, reviewerID: String,
                    reviewerName: String, summary: String, unixReviewTime: Long)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("Spark Search Examples").getOrCreate()
    import spark.implicits._

    // Amazon computers reviews
    println("Downloading amazon computers reviews file...")
    val computersReviewFile = File.createTempFile("reviews_Computers", ".json.gz")
    computersReviewFile.deleteOnExit()
    new URL("http://snap.stanford.edu/data/amazon/productGraph/categoryFiles/reviews_Computers.json.gz") #> computersReviewFile !!

    println("Amazon computers reviews file downloaded, loading...")

    // Amazon computers review
    // Number of partition is the number of Lucene index which will be created
    val computersReviews = spark.read.json(computersReviewFile.getAbsolutePath).as[Review].cache
      // Number of partition is the number of Lucene index which will be created across your cluster
      .repartition(4)
    println(s"${computersReviews.count} amazon computers reviews loaded, indexing...")

    // Search SQL API
    // import org.apache.spark.search.sql._ to implicitly enhance Dataset with search features

    // Count positive review: indexation + count matched doc
    val happyReview = computersReviews.count("reviewText:happy OR reviewText:best or reviewText:good")
    println(s"${happyReview} positive reviews :)")

    // Search for key words
    println(s"Full text search results:")
    computersReviews.searchList("reviewText:\"World of Warcraft\" OR reviewText:\"Civilization IV\"", 100)
      .foreach(println)

    // /!\ Important lucene indexation is done each time a SearchRDD is computed,
    // if you do multiple operations on the same parent RDD, you might a variable in the driver:
    val computersReviewsSearch = computersReviews.search(
      "(RAM or memory) and (CPU or processor)^4",
      topKByPartition = 10,
      opts = SearchOptions.builder[Review]() // See all other options SearchOptions, IndexationOptions and ReaderOptions
        .read((r: ReaderOptions.Builder[Review]) => r.defaultFieldName("reviewText"))
        .analyzer(classOf[EnglishAnalyzer])
        .build())
    println("All reviews speaking about hardware:")
    computersReviewsSearch.foreach(println(_))

    // Amazon software reviews
    println("Downloading amazon software reviews file...")
    val softwareReviewsFile = File.createTempFile("reviews_Software", ".json.gz")
    softwareReviewsFile.deleteOnExit()
    new URL("http://snap.stanford.edu/data/amazon/productGraph/categoryFiles/reviews_Software_10.json.gz") #> softwareReviewsFile !!
    val softwareReviews = spark.read.json(softwareReviewsFile.getAbsolutePath).as[Review].cache.filter(_.reviewerName != null)

    println("Downloaded amazon software reviews file, matching reviewer against computers:")
    // Match software and computer reviewers
    val matchesReviewers = computersReviews.searchJoin(softwareReviews,
      (sr: Review) => s"reviewerName:${"\"" + sr.reviewerName.replace('"', ' ') + "\""}~8", 10).cache

    matchesReviewers
      .filter(_.hits.nonEmpty)
      .map(m => (m.doc.reviewerName, m.hits.map(h => (h.source.reviewerName, h.score))))
      .foreach(println(_))

    matchesReviewers.write.mode(SaveMode.Overwrite).json("reviews-matched-ds")

    spark.close()
  }
}
