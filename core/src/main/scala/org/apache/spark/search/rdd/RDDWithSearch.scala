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

import org.apache.lucene.search.Query
import org.apache.spark.rdd.RDD
import org.apache.spark.search.SearchOptions
import org.apache.spark.search._

import scala.reflect.ClassTag

/**
 * Add search features to [[org.apache.spark.rdd.RDD]]
 * using [[org.apache.spark.search.rdd.SearchRDD]].
 *
 * @author Pierrick HYMBERT
 */
private[rdd] class RDDWithSearch[T: ClassTag](val rdd: RDD[T]) {

  /**
   * Count how many documents match the given query.
   *
   * @param query Matching query
   * @return Matched doc count
   */
  def count(query: String): Long =
    searchRDD.countQuery(parseQueryString(query))

  /**
   * Count how many documents match the given query.
   *
   * @param query Matching query
   * @param opts  Search options
   * @return Matched doc count
   */
  def count(query: String, opts: SearchOptions[T]): Long =
    searchRDD(opts).countQuery(parseQueryString(query))

  /**
   * Count how many documents match the given query.
   *
   * @param query Matching query
   * @return Matched doc count
   */
  def countQuery(query: () => Query): Long =
    searchRDD.countQuery(query)

  /**
   * Count how many documents match the given query.
   *
   * @param query Matching query
   * @param opts  Search options
   * @return Matched doc count
   */
  def countQuery(query: () => Query, opts: SearchOptions[T]): Long =
    searchRDD(opts).countQuery(query)

  /**
   * Finds the top topK hits for query.
   *
   * @param query Lucene query syntax
   * @return matching hits
   * @note this method should only be used if the topK is expected to be small, as
   *       all the data is loaded into the driver's memory.
   */
  def searchList(query: String): Array[SearchRecord[T]] =
    searchRDD.searchQueryList(parseQueryString(query))

  /**
   * Finds the top topK hits for query.
   *
   * @param query Lucene query syntax
   * @param topK  topK to return
   * @return topK hits
   * @note this method should only be used if the topK is expected to be small, as
   *       all the data is loaded into the driver's memory.
   */
  def searchList(query: String,
                 topK: Int): Array[SearchRecord[T]] =
    searchRDD.searchQueryList(parseQueryString(query), topK)

  /**
   * Finds the top topK hits for query.
   *
   * @param query    Lucene query syntax
   * @param topK     topK to return
   * @param minScore minimum score of matching documents
   * @return topK hits
   * @note this method should only be used if the topK is expected to be small, as
   *       all the data is loaded into the driver's memory.
   */
  def searchList(query: String,
                 topK: Int,
                 minScore: Double): Array[SearchRecord[T]] =
    searchRDD.searchQueryList(parseQueryString(query), topK, minScore)

  /**
   * Finds the top topK hits for query.
   *
   * @param query    Lucene query syntax
   * @param topK     topK to return
   * @param minScore minimum score of matching documents
   * @param opts     Search options
   * @return topK hits
   * @note this method should only be used if the topK is expected to be small, as
   *       all the data is loaded into the driver's memory.
   */
  def searchList(query: String,
                 topK: Int,
                 minScore: Double,
                 opts: SearchOptions[T]): Array[SearchRecord[T]] =
    searchRDD(opts).searchQueryList(parseQueryString(query), topK, minScore)

  /**
   * Finds the top topK hits for query.
   *
   * @param query Lucene query syntax
   * @return matching hits
   * @note this method should only be used if the topK is expected to be small, as
   *       all the data is loaded into the driver's memory.
   */
  def searchQueryList(query: () => Query): Array[SearchRecord[T]] =
    searchRDD.searchQueryList(query)

  /**
   * Finds the top topK hits for query.
   *
   * @param query Lucene query syntax
   * @param topK  topK to return
   * @return topK hits
   * @note this method should only be used if the topK is expected to be small, as
   *       all the data is loaded into the driver's memory.
   */
  def searchQueryList(query: () => Query,
                 topK: Int): Array[SearchRecord[T]] =
    searchRDD.searchQueryList(query, topK)

  /**
   * Finds the top topK hits for query.
   *
   * @param query    Lucene query syntax
   * @param topK     topK to return
   * @param minScore minimum score of matching documents
   * @return topK hits
   * @note this method should only be used if the topK is expected to be small, as
   *       all the data is loaded into the driver's memory.
   */
  def searchQueryList(query: () => Query,
                 topK: Int,
                 minScore: Double): Array[SearchRecord[T]] =
    searchRDD.searchQueryList(query, topK, minScore)

  /**
   * Finds the top topK hits for query.
   *
   * @param query    Lucene query syntax
   * @param topK     topK to return
   * @param minScore minimum score of matching documents
   * @param opts     Search options
   * @return topK hits
   * @note this method should only be used if the topK is expected to be small, as
   *       all the data is loaded into the driver's memory.
   */
  def searchQueryList(query: () => Query,
                 topK: Int,
                 minScore: Double,
                 opts: SearchOptions[T]): Array[SearchRecord[T]] =
    searchRDD(opts).searchQueryList(query, topK, minScore)

  /**
   * Finds the top topK hits for query.
   *
   * @param query Lucene query syntax
   * @return matching hits RDD
   */
  def search(query: String): RDD[SearchRecord[T]] =
    searchRDD.search(query)

  /**
   * Finds the top topK hits for query.
   *
   * @param query           Lucene query syntax
   * @param topKByPartition topK to return
   * @return topK per partition hits RDD
   */
  def search(query: String,
             topKByPartition: Int): RDD[SearchRecord[T]] =
    searchRDD.search(query, topKByPartition)

  /**
   * Finds the top topK hits for query.
   *
   * @param query           Lucene query syntax
   * @param topKByPartition topK to return
   * @param minScore        minimum score of matching documents
   * @return topK per partition hits RDD
   */
  def search(query: String,
             topKByPartition: Int,
             minScore: Double): RDD[SearchRecord[T]] =
    searchRDD.search(query, topKByPartition, minScore)

  /**
   * Finds the top topK hits for query.
   *
   * @param query           Lucene query syntax
   * @param topKByPartition topK to return
   * @param opts            Search options
   * @return topK partition hits RDD
   */
  def search(query: String,
             topKByPartition: Int,
             minScore: Double,
             opts: SearchOptions[T]): RDD[SearchRecord[T]] =
    searchRDD(opts).search(query, topKByPartition, minScore)

  /**
   * Finds the top topK hits for query.
   *
   * @param query Lucene query syntax
   * @return matching hits RDD
   */
  def searchQuery(query: () => Query): RDD[SearchRecord[T]] =
    searchRDD.searchQuery(query)

  /**
   * Finds the top topK hits for query.
   *
   * @param query           Lucene query syntax
   * @param topKByPartition topK to return
   * @return topK hits per partition RDD
   */
  def searchQuery(query: () => Query,
             topKByPartition: Int): RDD[SearchRecord[T]] =
    searchRDD.searchQuery(query, topKByPartition)

  /**
   * Finds the top topK hits for query.
   *
   * @param query           Lucene query syntax
   * @param topKByPartition topK to return
   * @param minScore        minimum score of matching documents
   * @return topK hits per partition RDD
   */
  def searchQuery(query: () => Query,
             topKByPartition: Int,
             minScore: Double): RDD[SearchRecord[T]] =
    searchRDD.searchQuery(query, topKByPartition, minScore)

  /**
   * Finds the top topK hits for query.
   *
   * @param query           Lucene query syntax
   * @param topKByPartition topK to return
   * @param minScore        minimum score of matching documents
   * @param opts            Search options
   * @return topK hits per partition RDD
   */
  def searchQuery(query: () => Query,
             topKByPartition: Int,
             minScore: Double,
             opts: SearchOptions[T]): RDD[SearchRecord[T]] =
    searchRDD(opts).searchQuery(query, topKByPartition, minScore)

  /**
   * Joins the input RDD against this one and returns matching hits.
   */
  def searchJoin[S: ClassTag](rdd: RDD[S], queryBuilder: S => String): RDD[Match[S, T]] =
    searchRDD.searchJoin(rdd, queryBuilder)

  /**
   * Joins the input RDD against this one and returns matching hits.
   */
  def searchJoin[S: ClassTag](rdd: RDD[S],
                              queryBuilder: S => String,
                              topK: Int): RDD[Match[S, T]] =
    searchRDD.searchJoin(rdd, queryBuilder, topK)

  /**
   * Joins the input RDD against this one and returns matching hits.
   */
  def searchJoin[S: ClassTag](rdd: RDD[S],
                              queryBuilder: S => String,
                              topK: Int,
                              minScore: Double,
                              opts: SearchOptions[T]): RDD[Match[S, T]] =
    searchRDD(opts).searchJoin(rdd, queryBuilder, topK, minScore)

  /**
   * Joins the input RDD against this one and returns matching hits.
   */
  def searchQueryJoin[S: ClassTag](rdd: RDD[S], queryBuilder: S => Query): RDD[Match[S, T]] =
    searchRDD.searchQueryJoin(rdd, queryBuilder)

  /**
   * Joins the input RDD against this one and returns matching hits.
   */
  def searchQueryJoin[S: ClassTag](rdd: RDD[S],
                              queryBuilder: S => Query,
                              topK: Int): RDD[Match[S, T]] =
    searchRDD.searchQueryJoin(rdd, queryBuilder, topK)

  /**
   * Joins the input RDD against this one and returns matching hits.
   */
  def searchQueryJoin[S: ClassTag](rdd: RDD[S],
                                   queryBuilder: S => Query,
                                   topK: Int,
                                   minScore: Double): RDD[Match[S, T]] =
    searchRDD.searchQueryJoin(rdd, queryBuilder, topK, minScore)

  /**
   * Joins the input RDD against this one and returns matching hits.
   */
  def searchQueryJoin[S: ClassTag](rdd: RDD[S],
                              queryBuilder: S => Query,
                              topK: Int,
                              minScore: Double,
                              opts: SearchOptions[T]): RDD[Match[S, T]] =
    searchRDD(opts).searchQueryJoin(rdd, queryBuilder, topK, minScore)

  /**
   * @return Dependent RDD with search features
   */
  def searchRDD: SearchRDD[T] = searchRDD(defaultOpts)

  /**
   * @param opts Search options
   * @return Dependent RDD with configurable search features
   */
  def searchRDD(opts: SearchOptions[T]): SearchRDD[T] = new SearchRDD[T](rdd, opts)

  sealed trait QueryBuilder[S] extends (S => Query)

}
