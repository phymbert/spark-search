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

import org.apache.spark.annotation.InterfaceStability
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
 * Add search features to rdd.
 */
@InterfaceStability.Unstable
private[rdd] class RDDWithSearch[T: ClassTag](val rdd: RDD[T]) {

  /**
   * @return Dependent RDD with search features
   */
  @InterfaceStability.Stable
  def searchRDD: SearchRDD[T] = searchRDD(SearchRDDOptions.defaultOptions())

  /**
   * @param opts Search options
   * @return Dependent RDD with configurable search features
   */
  @InterfaceStability.Unstable
  def searchRDD(opts: SearchRDDOptions[T]) = new SearchRDD[T](rdd, opts)

  /**
   * Finds the top topK hits for query.
   * @param query Lucene query syntax
   * @param topK topK to return
   * @param opts Search options
   * @return topK hits
   */
  @InterfaceStability.Unstable
  def search(query: String, topK: Int, opts: SearchRDDOptions[T] = SearchRDDOptions.defaultOptions()): List[SearchRecord[T]] =
    searchRDD(opts).search(query, topK)

  /**
   * Count how many documents match the given query.
   *
   * @param query Lucene query syntax
   * @return Matched doc count
   */
  @InterfaceStability.Stable
  def count(query: String): Long = count(query, defaultOpts)

  /**
   * Count how many documents match the given query.
   *
   * @param query Lucene query syntax
   * @param opts Search options
   * @return Matched doc count
   */
  @InterfaceStability.Unstable
  def count(query: String, opts: SearchRDDOptions[T] = defaultOpts): Long =
    searchRDD(opts)
      .count(query)

  protected def defaultOpts: SearchRDDOptions[T] = SearchRDDOptions.defaultOptions.asInstanceOf[SearchRDDOptions[T]]
}
