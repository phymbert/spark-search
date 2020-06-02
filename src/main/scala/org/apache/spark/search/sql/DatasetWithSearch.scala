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
package org.apache.spark.search.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.search.rdd.{Match, SearchRDDOptions, SearchRecord, _}
import org.apache.spark.sql.{Dataset, Encoder}

import scala.reflect.ClassTag

/**
 * Dataset with search features.
 *
 * @author Pierrick HYMBERT
 */
class DatasetWithSearch[T: ClassTag](dataset: Dataset[T]) {

  /**
   * [[org.apache.spark.search.rdd.SearchRDD#count(SearchQuery)]]
   */
  def count(query: SearchQuery, opts: SearchRDDOptions[T] = defaultOpts): Long =
    searchRDD(opts).count(query)

  /**
   * [[org.apache.spark.search.rdd.SearchRDD#searchList(SearchQuery, int, float)]]
   */
  def searchList(query: SearchQuery,
                 topK: Int = Int.MaxValue,
                 minScore: Float = 0,
                 opts: SearchRDDOptions[T] = defaultOpts): Array[SearchRecord[T]] =
    searchRDD(opts).searchList(query, topK, minScore)

  /**
   * [[org.apache.spark.search.rdd.SearchRDD#search(SearchQuery, int, float)]]
   */
  def search(query: SearchQuery,
             topKByPartition: Int = Int.MaxValue,
             minScore: Float = 0,
             opts: SearchRDDOptions[T] = defaultOpts): Dataset[SearchRecord[T]] =
    asDS(searchRDD(opts).search(query, topKByPartition, minScore))

  /**
   * [[org.apache.spark.search.rdd.SearchRDD#searchJoin(org.apache.spark.rdd.RDD, scala.Function1, int, float)]]
   */
  def searchJoin[S](dataset: Dataset[S],
                    queryBuilder: S => SearchQuery,
                    topK: Int = Int.MaxValue,
                    minScore: Float = 0,
                    opts: SearchRDDOptions[T] = defaultOpts): Dataset[Match[S, T]] =
    asDS(searchRDD(opts).searchJoin(dataset.rdd, queryBuilder, topK, minScore))

  /**
   * @param opts Search options
   * @return Dependent RDD with configurable search features
   */
  def searchRDD(opts: SearchRDDOptions[T]): SearchRDD[T] = new SearchRDD[T](dataset.rdd, opts)

  protected def asDS[R: Encoder](rdd: RDD[R]): Dataset[R] = {
    dataset.sparkSession.createDataset[R](rdd)
  }
}
