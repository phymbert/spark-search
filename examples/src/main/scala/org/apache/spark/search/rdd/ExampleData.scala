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

import java.io.File
import java.net.URL

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.language.postfixOps
import scala.sys.process._

object ExampleData {

  case class Review(asin: String, helpful: Array[Long], overall: Double,
                    reviewText: String, reviewTime: String, reviewerID: String,
                    reviewerName: String, summary: String, unixReviewTime: Long)

  def loadComputerReviews(spark: SparkSession): (RDD[Review], RDD[Review]) = {
    import spark.implicits._

    val hadoopConf = new Configuration()
    val hdfs = FileSystem.get(hadoopConf)

    // Amazon computers reviews
    println("Downloading amazon computers reviews...")
    val computersReviewFile = File.createTempFile("reviews_Computers", ".json.gz")
    computersReviewFile.deleteOnExit()
    new URL("http://snap.stanford.edu/data/amazon/productGraph/categoryFiles/reviews_Computers.json.gz") #> computersReviewFile !!

    hdfs.copyFromLocalFile(new Path(computersReviewFile.getAbsolutePath), new Path("/tmp/reviews_Computers.json.gz"))

    val computersReviewsRDD = spark.read.json("/tmp/reviews_Computers.json.gz").as[Review].rdd.repartition(4)

    // Amazon software reviews
    println("Downloading amazon software reviews...")
    val softwareReviewsFile = File.createTempFile("reviews_Software", ".json.gz")
    softwareReviewsFile.deleteOnExit()
    new URL("http://snap.stanford.edu/data/amazon/productGraph/categoryFiles/reviews_Software_10.json.gz") #> softwareReviewsFile !!

    hdfs.copyFromLocalFile(new Path(softwareReviewsFile.getAbsolutePath), new Path("/tmp/reviews_Software.json.gz"))

    val softwareReviewsRDD = spark.read.json("/tmp/reviews_Software.json.gz").as[Review].rdd.repartition(4)

    (computersReviewsRDD, softwareReviewsRDD)
  }
}
