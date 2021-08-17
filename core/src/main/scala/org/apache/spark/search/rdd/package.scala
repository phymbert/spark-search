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
package org.apache.spark.search

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.language.implicitConversions
import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

/**
 * Spark Search RDD.
 */
package object rdd {

  implicit def rddWithSearch[T: ClassTag](rdd: RDD[T]): RDDWithSearch[T] = new RDDWithSearch[T](rdd)

  /**
   * Reload an indexed RDD from spark FS.
   *
   * @param sc      current spark context
   * @param path    Path where the index were saved
   * @param options Search option
   * @tparam T Type of beans or case class this RDD is binded to
   * @return Restored RDD
   */
  def loadSearchRDD[T: ClassTag](sc: SparkContext,
                                 path: String,
                                 options: SearchOptions[T] = defaultOpts[T]
                                ): SearchRDD[T] =
    new SearchRDDLucene[T](sc, new SearchIndexReloadedRDD[T](sc, path, options), options, Nil)

  private[rdd] def searchRecordJavaToProduct[T](sr: SearchRecordJava[T]) = {
    SearchRecord(sr.id, sr.partitionIndex, sr.score, sr.shardIndex, sr.source)
  }

  private[rdd] def tryAndClose[A <: AutoCloseable, B](resource: A)(block: A => B): B = {
    Try(block(resource)) match {
      case Success(result) =>
        resource.close()
        result
      case Failure(e) =>
        resource.close()
        throw e
    }
  }
}
