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

import org.apache.lucene.search.Query
import org.apache.spark.rdd.RDD

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
    searchRDD.count(query)

  /**
   * Count how many documents match the given query.
   *
   * @param query Matching query
   * @param opts  Search options
   * @return Matched doc count
   */
  def count(query: String, opts: SearchRDDOptions[T]): Long =
    searchRDD(opts).count(query)

  /**
   * Finds the top topK hits for query.
   *
   * @param query Lucene query syntax
   * @return matching hits
   * @note this method should only be used if the topK is expected to be small, as
   *       all the data is loaded into the driver's memory.
   */
  def searchList(query: String): Array[SearchRecord[T]] =
    searchRDD.searchList(query)

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
    searchRDD.searchList(query, topK)

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
    searchRDD.searchList(query, topK, minScore)

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
                 opts: SearchRDDOptions[T]): Array[SearchRecord[T]] =
    searchRDD(opts).searchList(query, topK, minScore)

  /**
   * Joins the input RDD against this one and returns matching hits.
   */
  def searchJoin[S](rdd: RDD[S], queryBuilder: S => String): RDD[Match[S, T]] =
    searchRDD.searchJoin(rdd, s => SearchQueryString(queryBuilder.apply(s)))

  /**
   * Joins the input RDD against this one and returns matching hits.
   */
  def searchJoin[S](rdd: RDD[S],
                    queryBuilder: S => String,
                    topK: Int): RDD[Match[S, T]] =
    searchRDD.searchJoin(rdd, s => SearchQueryString(queryBuilder.apply(s)), topK)

  /**
   * Joins the input RDD against this one and returns matching hits.
   */
  def searchJoin[S](rdd: RDD[S],
                    queryBuilder: S => String,
                    topK: Int,
                    minScore: Double,
                    opts: SearchRDDOptions[T]): RDD[Match[S, T]] =
    searchRDD(opts).searchJoin(rdd, s => SearchQueryString(queryBuilder.apply(s)), topK, minScore)

  /**
   * Count how many documents match the given query.
   *
   * @param query Matching query
   * @return Matched doc count
   */
  def count(query: Query): Long =
    searchRDD.count(query)

  /**
   * Count how many documents match the given query.
   *
   * @param query Matching query
   * @param opts  Search options
   * @return Matched doc count
   */
  def count(query: Query, opts: SearchRDDOptions[T]): Long =
    searchRDD(opts).count(query)

  /**
   * Finds the top topK hits for query.
   *
   * @param query Lucene query syntax
   * @return matching hits
   * @note this method should only be used if the topK is expected to be small, as
   *       all the data is loaded into the driver's memory.
   */
  def searchList(query: Query): Array[SearchRecord[T]] =
    searchRDD.searchList(query)

  /**
   * Finds the top topK hits for query.
   *
   * @param query Lucene query syntax
   * @param topK  topK to return
   * @return topK hits
   * @note this method should only be used if the topK is expected to be small, as
   *       all the data is loaded into the driver's memory.
   */
  def searchList(query: Query,
                 topK: Int): Array[SearchRecord[T]] =
    searchRDD.searchList(query, topK)

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
  def searchList(query: Query,
                 topK: Int,
                 minScore: Double): Array[SearchRecord[T]] =
    searchRDD.searchList(query, topK, minScore)

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
  def searchList(query: Query,
                 topK: Int,
                 minScore: Double,
                 opts: SearchRDDOptions[T]): Array[SearchRecord[T]] =
    searchRDD(opts).searchList(query, topK, minScore)

  /**
   * Joins the input RDD against this one and returns matching hits.
   */
  def searchJoin[S](rdd: RDD[S], queryBuilder: QueryBuilder[S]): RDD[Match[S, T]] =
    searchRDD.searchJoin(rdd, s => SearchLuceneQuery(queryBuilder.apply(s)))

  /**
   * Joins the input RDD against this one and returns matching hits.
   */
  def searchJoin[S](rdd: RDD[S],
                    queryBuilder: QueryBuilder[S],
                    topK: Int): RDD[Match[S, T]] =
    searchRDD.searchJoin(rdd, s => SearchLuceneQuery(queryBuilder.apply(s)), topK)

  /**
   * Joins the input RDD against this one and returns matching hits.
   */
  def searchJoin[S](rdd: RDD[S],
                    queryBuilder: QueryBuilder[S],
                    topK: Int,
                    minScore: Double,
                    opts: SearchRDDOptions[T]): RDD[Match[S, T]] =
    searchRDD(opts).searchJoin(rdd, s => SearchLuceneQuery(queryBuilder.apply(s)), topK, minScore)

  /**
   * Finds all hits per partition for query.
   */
  def search(query: String): RDD[SearchRecord[T]] =
    searchRDD.search(query)

  /**
   * Finds the top topK hits per partition for query.
   */
  def search(query: String, topKByPartition: Int): RDD[SearchRecord[T]] =
    searchRDD.search(query, topKByPartition)

  /**
   * Finds the top topK hits per partition for query.
   */
  def search(query: String, topKByPartition: Int, minScore: Double): RDD[SearchRecord[T]] =
    searchRDD.search(query, topKByPartition, minScore)

  /**
   * Finds the top topK hits per partition for query.
   */
  def search(query: String, topKByPartition: Int, minScore: Double,
             opts: SearchRDDOptions[T]): RDD[SearchRecord[T]] =
    searchRDD(opts).search(query, topKByPartition, minScore)

  /**
   * Finds all hits per partition for query.
   */
  def search(query: Query): RDD[SearchRecord[T]] =
    searchRDD.search(query)

  /**
   * Finds the top topK hits per partition for query.
   */
  def search(query: Query, topKByPartition: Int): RDD[SearchRecord[T]] =
    searchRDD.search(query, topKByPartition)

  /**
   * Finds the top topK hits per partition for query.
   */
  def search(query: Query, topKByPartition: Int, minScore: Double): RDD[SearchRecord[T]] =
    searchRDD.search(query, topKByPartition, minScore)

  /**
   * Finds the top topK hits per partition for query.
   */
  def search(query: Query, topKByPartition: Int, minScore: Double,
             opts: SearchRDDOptions[T]): RDD[SearchRecord[T]] =
    searchRDD(opts).search(query, topKByPartition, minScore)

  /**
   * @return Dependent RDD with search features
   */
  def searchRDD: SearchRDD[T] = searchRDD(defaultOpts)

  /**
   * @param opts Search options
   * @return Dependent RDD with configurable search features
   */
  def searchRDD(opts: SearchRDDOptions[T]): SearchRDD[T] = new SearchRDD[T](rdd, opts)

  sealed trait QueryBuilder[S] extends (S => Query)

}
