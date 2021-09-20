/*
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
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.search._

import scala.reflect.ClassTag


/**
 * A search RDD Spark Search brings
 * advanced full text search features to your RDD.
 *
 * {@link org.apache.spark.search.rdd.SearchRDDLucene}
 *
 * @tparam S Doc type to index
 * @author Pierrick HYMBERT
 */
trait SearchRDD[S] {
  /**
   * Return the number of indexed elements in the RDD.
   */
  def count(): Long

  /**
   * Count how many documents match the given text query.
   *
   * @param query Matching query
   * @return Matched doc count
   */
  def count(query: String): Long =
    count(parseQueryString(query, options))

  /**
   * Count how many documents match the given lucene query.
   *
   * @param query Query to match
   * @return Matched doc count
   */
  def count(query: StaticQueryProvider): Long

  /**
   * Searches the top k hits for this query string.
   *
   * @param query    Lucene query syntax
   * @param topK     topK to return
   * @param minScore minimum score of matching documents
   * @return topK hits collected to the driver as an array
   * @note this method should only be used if the topK is expected to be small, as
   *       all the data is loaded into the driver's memory.
   */
  def searchList(query: String,
                 topK: Int = Int.MaxValue,
                 minScore: Double = 0): Array[SearchRecord[S]]
  = searchListQuery(parseQueryString(query, options), topK, minScore)

  /**
   * Searches the top topK hits for this Lucene query.
   *
   * @param query    Lucene query syntax
   * @param topK     topK to return
   * @param minScore minimum score of matching documents
   * @return topK hits
   * @note this method should only be used if the topK is expected to be small, as
   *       all the data is loaded into the driver's memory.
   */
  def searchListQuery(query: StaticQueryProvider,
                      topK: Int = Int.MaxValue,
                      minScore: Double = 0): Array[SearchRecord[S]]

  /**
   * Searches for the top K hits
   * per partition for this query string
   * and returns an RDD with all hits sorted by score in descendent order.
   *
   * @param query           Lucene query syntax
   * @param topKByPartition topK to return per partition
   * @param minScore        minimum score of matching documents
   * @return topK per partition hits RDD sorted by score in descendent order
   */
  def search(query: String,
             topKByPartition: Int = Int.MaxValue,
             minScore: Double = 0): RDD[SearchRecord[S]] =
    searchQuery(parseQueryString(query, options), topKByPartition, minScore)

  /**
   * Searches for the top K hits per partition for this lucene query
   * and returns an RDD with all hits sorted by score in descendent order.
   *
   * @param query           Lucene query
   * @param topKByPartition topK to return per partition
   * @param minScore        minimum score of matching documents
   * @return topK per partition hits RDD sorted by score in descendent order
   */
  def searchQuery(query: StaticQueryProvider,
                  topKByPartition: Int = Int.MaxValue,
                  minScore: Double = 0): RDD[SearchRecord[S]]

  /**
   * Searches join for this input RDD elements matches against these ones
   * by building a lucene query string per doc
   * and returns matching hit as tuples.
   *
   * @param rdd             to match with
   * @param queryBuilder    builds the query string to match with the searched document
   * @param topKByPartition – topK to return by partition
   * @param minScore        minimum score of matching documents
   * @tparam S Doc type to match with
   * @return matches doc and related hit RDD
   */
  def searchJoin[K: ClassTag, V: ClassTag](rdd: RDD[(K, V)],
                                           queryBuilder: V => String,
                                           topKByPartition: Int = Int.MaxValue,
                                           minScore: Double = 0): RDD[(K, (V, SearchRecord[S]))] =
    searchJoinQuery(rdd, queryStringBuilder(queryBuilder, options), topKByPartition, minScore)

  /**
   * Searches join for this input RDD elements matches against these ones
   * by building a lucene query string per doc
   * and returns matching hit as tuples.
   *
   * @param rdd             to match with
   * @param queryBuilder    builds the query string to match with the searched document
   * @param topKByPartition – topK to return by partition
   * @param minScore        minimum score of matching documents
   * @tparam S Doc type to match with
   * @return matches doc and related hit RDD
   */
  def searchJoinQuery[K: ClassTag, V: ClassTag](rdd: RDD[(K, V)],
                                                queryBuilder: V => Query,
                                                topKByPartition: Int = Int.MaxValue,
                                                minScore: Double = 0): RDD[(K, (V, SearchRecord[S]))]

  /**
   * Searches for this input RDD elements matches against these ones
   * by building a lucene query string per doc
   * and returns matching hits per doc.
   *
   * @param rdd          to match with
   * @param queryBuilder builds the query string to match with the searched document
   * @param topK         topK to return
   * @param minScore     minimum score of matching documents
   * @tparam S Doc type to match with
   * @return matches doc and related hits RDD
   */
  def matches[K: ClassTag, V: ClassTag](rdd: RDD[(K, V)],
                                        queryBuilder: V => String,
                                        topK: Int = Int.MaxValue,
                                        minScore: Double = 0): RDD[DocAndHits[V, S]] =
    matchesQuery(rdd, queryStringBuilder(queryBuilder, options), topK, minScore)

  /**
   * Searches for this input RDD elements matches against these ones
   * by building a lucene query per doc
   * and returns matching hits per doc.
   *
   * @param rdd          to match with
   * @param queryBuilder builds the lucene query to join with the searched document
   * @param topK         topK to return
   * @param minScore     minimum score of matching documents
   * @tparam S Doc type to match with
   * @return matches doc and related hits RDD
   */
  def matchesQuery[K: ClassTag, V: ClassTag](rdd: RDD[(K, V)],
                                             queryBuilder: V => Query,
                                             topK: Int = Int.MaxValue,
                                             minScore: Double = 0): RDD[DocAndHits[V, S]]

  /**
   * Alias for
   * [[org.apache.spark.search.rdd.SearchRDD#searchDropDuplicates(scala.Function1, int, double, int)}]]
   */
  def distinct(numPartitions: Int): RDD[S] =
    searchDropDuplicates(numPartitions = numPartitions)

  /**
   * Drops duplicated records by applying lookup for matching hits of the query against this RDD.
   *
   * @param queryBuilder  builds the lucene query to search for duplicate
   * @param topK          topK to return
   * @param minScore      minimum score of matching documents
   * @param numPartitions num partition of the
   */
  def searchDropDuplicates(queryBuilder: S => Query = defaultQueryBuilder(options),
                           topK: Int = Int.MaxValue,
                           minScore: Double = 0,
                           numPartitions: Int = getNumPartitions): RDD[S]

  /**
   * Save the current indexed RDD onto hdfs
   * in order to be able to reload it later on.
   *
   * @param path Path on the spark file system (hdfs) to save on
   */
  def save(path: String): Unit

  /**
   * @return the number of partitions of this RDD.
   */
  def getNumPartitions: Int

  /**
   * @return current search options
   */
  def options: SearchOptions[S]
}

object SearchRDD {
  /**
   * Reload an indexed RDD from spark FS.
   *
   * @param sc      current spark context
   * @param path    Path where the index were saved
   * @param options Search option
   * @tparam T Type of beans or case class this RDD is binded to
   * @return Restored RDD
   */
  def load[T: ClassTag](sc: SparkContext,
                        path: String,
                        options: SearchOptions[T] = defaultOpts[T]
                       ): SearchRDD[T] =
    new SearchRDDLucene[T](sc, new SearchIndexReloadedRDD[T](sc, path, options), options, Nil)
}