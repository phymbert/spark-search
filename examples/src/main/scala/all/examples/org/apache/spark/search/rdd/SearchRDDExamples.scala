/**
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
import org.apache.spark.rdd.RDD

/**
 * Spark Search RDD examples.
 */
object SearchRDDExamples {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Spark Search Examples")
      .config("spark.default.parallelism", "4")
      .config("spark.sql.shuffle.partitions", "4")
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
      .foreach(println)

    // Amazon software customer reviews
    println("Downloading software reviews...")
    val softwareReviews: RDD[Review] = loadReviews(spark, "http://snap.stanford.edu/data/amazon/productGraph/categoryFiles/reviews_Software_10.json.gz")

    // Match software and computer reviewers
    println("Joined software and computer reviews by reviewer names:")
    val matchesReviewers: RDD[Match[Review, Review]] = computersReviews.searchJoin(softwareReviews.filter(_.reviewerName != null),
      (sr: Review) => "reviewerName:\"" + sr.reviewerName.replace('"', ' ') + "\"~0.4", 10)
    matchesReviewers
      .filter(_.hits.nonEmpty)
      .map(m => (s"Reviewer ${m.doc.reviewerName} reviews computer ${m.doc.asin} but also on software:",
                    m.hits.map(h => s"${h.source.reviewerName}=${h.score}=${h.source.asin}").toList))
      .collect()
      .foreach(println)

    // Drop duplicates
    println("Dropping duplicated reviewers:")
    val distinctReviewers: RDD[String] = computersReviews.filter(_.reviewerName != null).searchDropDuplicates(
      queryBuilder = queryStringBuilder(sr => "reviewerName:\"" + sr.reviewerName.replace('"', ' ') + "\"~0.4")
    ).map(sr => sr.reviewerName)
    distinctReviewers.collect().foreach(println)

    // Save & restore example
    println(s"Restoring from previous indexation:")
    computersReviews.save("/tmp/hdfs-pathname")
    val restoredSearchRDD: SearchRDD[Review] = SearchRDD.load[Review](sc, "/tmp/hdfs-pathname")
    val happyReview2 = restoredSearchRDD.count("reviewText:happy OR reviewText:best OR reviewText:good")
    println(s"$happyReview2 positive reviews after restoration")

    spark.stop()
  }
}
