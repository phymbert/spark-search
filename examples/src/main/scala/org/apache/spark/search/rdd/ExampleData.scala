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
package org.apache.spark.search.rdd

import org.apache.spark.SparkFiles
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object ExampleData {

  case class Review(asin: String, helpful: Array[Long], overall: Double,
                    reviewText: String, reviewTime: String, reviewerID: String,
                    reviewerName: String, summary: String, unixReviewTime: Long)

  def loadComputerReviews(spark: SparkSession): (RDD[Review], RDD[Review]) = {
    import spark.implicits._

    // Amazon computers reviews
    println("Downloading amazon computers reviews...")
    spark.sparkContext.addFile("http://snap.stanford.edu/data/amazon/productGraph/categoryFiles/reviews_Computers.json.gz")
    val computersReviewsRDD = spark.read.json(SparkFiles.get("reviews_Computers.json.gz")).as[Review].rdd.repartition(4)

    // Amazon software reviews
    println("Downloading amazon software reviews...")
    spark.sparkContext.addFile("http://snap.stanford.edu/data/amazon/productGraph/categoryFiles/reviews_Software_10.json.gz")
    val softwareReviewsRDD = spark.read.json(SparkFiles.get("reviews_Software_10.json.gz")).as[Review].rdd.repartition(4)

    (computersReviewsRDD, softwareReviewsRDD)
  }
}
