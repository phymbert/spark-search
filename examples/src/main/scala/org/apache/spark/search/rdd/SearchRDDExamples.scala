/*
 *    Copyright 2020 the Spark Search contributors
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package org.apache.spark.search.rdd

import org.apache.lucene.analysis.en.EnglishAnalyzer
import org.apache.spark.SparkContext
import org.apache.spark.search.{ReaderOptions, SearchOptions}

/**
 * Spark Search RDD examples.
 */
object SearchRDDExamples {

  case class Review(asin: String, helpful: Array[Long], overall: Double,
                    reviewText: String, reviewTime: String, reviewerID: String,
                    reviewerName: String, summary: String, unixReviewTime: Long)

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext()

    // Amazon computers reviews
    val computersReviewsRDD = sc.parallelize(Seq(Review("AAAAA", Array(3, 3), 3.0, "Ok, this is a good computer to play Civilization IV or World of Warcraft", "11 29, 2010", "XXXXX", "Patrick H.", "Ok for an average user, but not much else.", 1290988800)))
      // Number of partition is the number of Lucene index which will be created across your cluster
      .repartition(4)

    // Search RDD API
    // import org.apache.spark.search.rdd._ to implicitly enhance RDD with search features

    // Count positive review: indexation + count matched doc
    val happyReview = computersReviewsRDD.count("reviewText:happy OR reviewText:best or reviewText:good")
    println(s"${happyReview} positive reviews :)")

    // Search for key words
    println(s"Full text search results:")
    computersReviewsRDD.searchList("reviewText:\"World of Warcraft\" OR reviewText:\"Civilization IV\"", 100)
      .foreach(println)

    // /!\ Important lucene indexation is done each time a SearchRDD is computed,
    // if you do multiple operations on the same parent RDD, you might a variable in the driver:
    val computersReviewsSearchRDD = computersReviewsRDD.searchRDD(
      SearchOptions.builder[Review]() // See all other options SearchRDDOptions, IndexationOptions and ReaderOptions
        .read((r: ReaderOptions.Builder[Review]) => r.defaultFieldName("reviewText"))
        .analyzer(classOf[EnglishAnalyzer])
        .build())
    println("All reviews speaking about hardware:")
    computersReviewsSearchRDD.searchList("(RAM or memory) and (CPU or processor)^4", 10).foreach(println)

    // Fuzzy matching
    println("Some typo in names:")
    computersReviewsSearchRDD.search("reviewerName:Mikey~0.8 or reviewerName:Patrik~0.4 or reviewerName:jonh~0.2", 100)
      .map(doc => (doc.source.reviewerName, doc.score))
      .foreach(println)

    // Amazon software reviews
    val softwareReviewsRDD = sc.parallelize(Seq(Review("BBBB", Array(1), 4.0, "I use this and Ulead video studio 11.", "09 17, 2008", "YYYY", "Patrick Holtt", "Great, easy to use and user friendly.", 1221609600)))

    // Match software and computer reviewers
    val matchesReviewersRDD = computersReviewsSearchRDD.searchJoin(softwareReviewsRDD,
      (sr: Review) => s"reviewerName:${"\"" + sr.reviewerName.replace('"', ' ') + "\""}~8", 10)
    matchesReviewersRDD
      .filter(_.hits.nonEmpty)
      .map(m => (m.doc.reviewerName, m.hits.map(h => (h.source.reviewerName, h.score))))
      .foreach(println)

    sc.stop()
  }
}
