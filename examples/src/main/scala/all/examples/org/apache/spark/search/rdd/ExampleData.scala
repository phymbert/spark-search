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

  def loadReviews(spark: SparkSession, reviewURL: String): RDD[Review] = {
    import spark.implicits._

    val hadoopConf = new Configuration()
    val hdfs = FileSystem.get(hadoopConf)

    val reviewsFile = File.createTempFile("reviews_", ".json.gz")
    reviewsFile.deleteOnExit()
    new URL(reviewURL) #> reviewsFile !!

    val dstPathName = "/tmp/reviews.json.gz"
    hdfs.copyFromLocalFile(new Path(reviewsFile.getAbsolutePath), new Path(dstPathName))
    hdfs.deleteOnExit(new Path(dstPathName))
    spark.read.json(dstPathName).as[Review].rdd.repartition(4)
  }
}
