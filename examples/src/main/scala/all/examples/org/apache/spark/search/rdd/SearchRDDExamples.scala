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
import org.apache.spark.search.rdd._
import org.apache.spark.sql.SparkSession

import ExampleData._

/**
 * Spark Search RDD examples.
 */
object SearchRDDExamples {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Spark Search Examples").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")

    // Amazon computers customer reviews
    val computersReviewsRDD = loadReviews(spark, "http://snap.stanford.edu/data/amazon/productGraph/categoryFiles/reviews_Computers.json.gz")

    // Search RDD API
    // import org.apache.spark.search.rdd._ to implicitly enhance RDD with search features

    // Count positive review: indexation + count matched doc
    val happyReview = computersReviewsRDD.count("reviewText:happy OR reviewText:best or reviewText:good")
    Console.err.println(s"${happyReview} positive reviews :)")

    // Search for key words
    Console.err.println(s"Full text search results:")
    computersReviewsRDD.searchList("reviewText:\"World of Warcraft\" OR reviewText:\"Civilization IV\"", 10)
      .foreach(Console.err.println)

    // /!\ Important lucene indexation is done each time a SearchRDD is computed,
    // if you do multiple operations on the same parent RDD, you might have a variable in the driver:
    val computersReviewsSearchRDD = computersReviewsRDD.searchRDD(
      SearchOptions.builder[Review]() // See all other options SearchRDDOptions, IndexationOptions and ReaderOptions
        .read((r: ReaderOptions.Builder[Review]) => r.defaultFieldName("reviewText"))
        .analyzer(classOf[EnglishAnalyzer])
        .build())
    Console.err.println("All reviews speaking about hardware:")
    computersReviewsSearchRDD.searchList("(RAM or memory) and (CPU or processor)^4", 10).foreach(Console.err.println)

    // Fuzzy matching
    Console.err.println("Some typo in names:")
    computersReviewsSearchRDD.search("reviewerName:Mikey~0.8 or reviewerName:\"Patrik\"~0.4 or reviewerName:jonh~0.2", 10)
      .map(doc => (doc.source.reviewerName, doc.score))
      .foreach(Console.err.println)

    // Amazon software customer reviews
    Console.err.println("Downloading software reviews...")
    val softwareReviewsRDD = loadReviews(spark, "http://snap.stanford.edu/data/amazon/productGraph/categoryFiles/reviews_Software_10.json.gz")

    // Match software and computer reviewers
    Console.err.println("Joined software and computer reviews by reviewer names:")
    val matchesReviewersRDD = computersReviewsSearchRDD.searchJoin(softwareReviewsRDD.filter(_.reviewerName != null),
      (sr: Review) => s"reviewerName:${"\"" + sr.reviewerName.replace('"', ' ') + "\""}~0.4", 10)
    matchesReviewersRDD
      .filter(_.hits.nonEmpty)
      .map(m => (m.doc.reviewerName, m.hits.map(h => (h.source.reviewerName, h.score))))
      .foreach(Console.err.println)

    // Save & restore example
    Console.err.println(s"Restoring from previous indexation:")
    softwareReviewsRDD.searchRDD().save("/tmp/save-path")
    val restoredSearchRDD = loadSearchRDD[Review](sc, "/tmp/save-path")
    val happyReview2 = restoredSearchRDD.count("reviewText:happy OR reviewText:best or reviewText:good")
    Console.err.println(s"${happyReview2} positive reviews after restoration ^^")

    spark.stop()
  }
}
