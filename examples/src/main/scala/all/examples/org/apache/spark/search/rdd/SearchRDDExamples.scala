/*
 * Copyright © 2020 Spark Search (The Spark Search Contributors)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package all.examples.org.apache.spark.search.rdd

import org.apache.lucene.analysis.en.EnglishAnalyzer
import org.apache.spark.search._
import org.apache.spark.sql.SparkSession
import ExampleData._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD

/**
 * Spark Search RDD examples.
 */
object SearchRDDExamples {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Spark Search Examples")
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    Console.setOut(Console.err)

    // Amazon computers customer reviews
    val computersReviews: RDD[Review] = loadReviews(spark, "http://snap.stanford.edu/data/amazon/productGraph/categoryFiles/reviews_Computers.json.gz")

    // Search RDD API
    import org.apache.spark.search.rdd._ // to implicitly enhance RDD with search features

    // Count positive review: indexation + count matched doc
    val happyReview = computersReviews.count("reviewText:happy OR reviewText:best OR reviewText:good OR reviewText:\"sounds great\"~")
    println(s"$happyReview positive reviews :)")

    // Search for key words
    println(s"Full text search results:")
    computersReviews.searchList("reviewText:\"World of Warcraft\" OR reviewText:\"Civilization IV\"",
      topK = 10, minScore = 10)
      .foreach(println)

    // /!\ Important lucene indexation is done each time a SearchRDD is computed,
    // if you do multiple operations on the same parent RDD, you might have a variable in the driver:
    val computersReviewsSearchRDD: SearchRDD[Review] = computersReviews.searchRDD(
      SearchOptions.builder[Review]() // See all other options SearchRDDOptions, IndexationOptions and ReaderOptions
        .read((r: ReaderOptions.Builder[Review]) => r.defaultFieldName("reviewText"))
        .analyzer(classOf[EnglishAnalyzer])
        .build())
    println("All reviews speaking about hardware:")
    computersReviewsSearchRDD.searchList("(RAM OR memory) AND (CPU OR processor~)^4",
      topK = 10, minScore = 15).foreach(println)

    // Fuzzy matching
    println("Some typo in names:")
    computersReviews.search("(reviewerName:Mikey~0.8) OR (reviewerName:\"Patrik\"~0.4) OR (reviewerName:jonh~0.2)",
      topKByPartition = 10)
      .map(doc => s"${doc.source.reviewerName}=${doc.score}")
      .collect()
      .map(_.toLowerCase)
      .distinct
      .foreach(println)

    // Amazon software customer reviews
    println("Downloading software reviews...")
    val softwareReviews: RDD[Review] = loadReviews(spark, "http://snap.stanford.edu/data/amazon/productGraph/categoryFiles/reviews_Software_10.json.gz")

    // Match software and computer reviewers
    println("Joined software and computer reviews by reviewer names:")
    val matchesReviewers: RDD[(Review, Array[SearchRecord[Review]])] = computersReviews.matches[String, Review](
      softwareReviews.filter(_.reviewerName != null).map(sr => (sr.asin, sr)),
      (sr: Review) => "reviewerName:\"" + sr.reviewerName.replace('"', ' ') + "\"~0.4", 10)
      .values
    matchesReviewers
      .filter(_._2.nonEmpty)
      .sortBy(_._2.length, ascending = false)
      .map(m => (s"Reviewer ${m._1.reviewerName} reviews computer ${m._1.asin} but also on software:",
        m._2.map(h => s"${h.source.reviewerName}=${h.score}=${h.source.asin}").toList))
      .collect()
      .take(20)
      .foreach(println)

    // Drop duplicates
    println("Dropping duplicated reviewers:")
    val distinctReviewers: RDD[String] = computersReviews
      .filter(_.reviewerName != null)
      .searchDropDuplicates[Int, Review](
      queryBuilder = queryStringBuilder[Review](sr => "reviewerName:\"" + sr.reviewerName.replace('"', ' ') + "\"~0.4")
    ).map(sr => sr.reviewerName)
    distinctReviewers.collect().sorted.take(20).foreach(println)

    // Save & restore example
    FileSystem.get(new Configuration).delete(new Path("/hdfs-tmp/hdfs-pathname"), true)
    println(s"Saving index to hdfs...")
    computersReviews.save("/hdfs-tmp/hdfs-pathname")
    println(s"Restoring from previous indexation:")
    val restoredSearchRDD: SearchRDD[Review] = SearchRDD.load[Review](sc, "/hdfs-tmp/hdfs-pathname")
    val happyReview2 = restoredSearchRDD.count("reviewText:happy OR reviewText:best OR reviewText:good")
    println(s"$happyReview2 positive reviews after restoration")

    // Restored index can be use as classical rdd
    val topReviewer = restoredSearchRDD.map(r => (r.reviewerID, 1))
      .reduceByKey(_ + _)
      .sortBy(_._2, ascending = false)
      .take(1).head
    println(s"${topReviewer._1} has submitted ${topReviewer._2} reviews")

    val topReviewNameFiltered = restoredSearchRDD.filter(_.reviewerID.equals(topReviewer._1))
      .take(1).head
    println(s"He is named ${topReviewNameFiltered.reviewerName}")

    spark.stop()
  }
}
