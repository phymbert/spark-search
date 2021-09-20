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
import org.apache.spark.rdd.RDD
import org.apache.spark.search.{SearchOptions, _}

import scala.reflect.ClassTag

/**
 * Add search features to [[org.apache.spark.rdd.RDD]]
 * using [[org.apache.spark.search.rdd.SearchRDDLucene]].
 *
 * @author Pierrick HYMBERT
 */
private[rdd] class RDDWithSearch[T: ClassTag](val rdd: RDD[T],
                                              val opts: SearchOptions[T] = defaultOpts[T]
                                             ) extends SearchRDD[T] {

  private lazy val searchRDD: SearchRDD[T] = searchRDD(opts)

  override def count(): Long = searchRDD.count()

  override def count(query: StaticQueryProvider): Long = searchRDD.count(query)

  override def searchListQuery(query: StaticQueryProvider,
                               topK: Int = Int.MaxValue,
                               minScore: Double = 0): Array[SearchRecord[T]] =
    searchRDD.searchListQuery(query, topK, minScore)

  override def searchQuery(query: StaticQueryProvider,
                           topKByPartition: Int = Int.MaxValue,
                           minScore: Double = 0): RDD[SearchRecord[T]] =
    searchRDD.searchQuery(query, topKByPartition, minScore)


  override def matches[S: ClassTag](rdd: RDD[S],
                                    queryBuilder: S => String,
                                    topK: Int = Int.MaxValue,
                                    minScore: Double = 0
                                      ): RDD[DocAndHits[S, T]] =
    searchRDD.matches(rdd, queryBuilder, topK, minScore)

  override def matchesQuery[S: ClassTag](rdd: RDD[S],
                                    queryBuilder: S => Query,
                                    topK: Int = Int.MaxValue,
                                    minScore: Double = 0
                                           ): RDD[DocAndHits[S, T]] =
    searchRDD.matchesQuery(rdd, queryBuilder, topK, minScore)


  override def searchDropDuplicates(queryBuilder: T => Query = defaultQueryBuilder(options),
                                    topK: Int = Int.MaxValue,
                                    minScore: Double = 0,
                                    numPartitions: Int = getNumPartitions): RDD[T] =
    searchRDD.searchDropDuplicates(queryBuilder, topK, minScore, numPartitions)

  override def save(path: String): Unit = searchRDD.save(path)

  override def getNumPartitions: Int = searchRDD.getNumPartitions

  override def options: SearchOptions[T] = searchRDD.options

  /**
   * Builds a search rdd with that custom search options.
   *
   * @param opts Search options
   * @return Dependent RDD with configurable search features
   */
  def searchRDD(opts: SearchOptions[T] = defaultOpts): SearchRDD[T] = new SearchRDDLucene[T](rdd, opts)
}
