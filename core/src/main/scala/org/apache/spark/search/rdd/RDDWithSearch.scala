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
private[rdd] class RDDWithSearch[S: ClassTag](val rdd: RDD[S],
                                              val opts: SearchOptions[S] = defaultOpts[S]
                                             ) extends SearchRDD[S] {

  private[rdd] lazy val _searchRDD: SearchRDD[S] = searchRDD(opts)

  override def count(): Long = _searchRDD.count()

  override def count(query: StaticQueryProvider): Long = _searchRDD.count(query)

  override def searchListQuery(query: StaticQueryProvider,
                               topK: Int = defaultTopK,
                               minScore: Double = 0): Array[SearchRecord[S]] =
    _searchRDD.searchListQuery(query, topK, minScore)

  override def searchQuery(query: StaticQueryProvider,
                           topKByPartition: Int = defaultTopK,
                           minScore: Double = 0): RDD[SearchRecord[S]] =
    _searchRDD.searchQuery(query, topKByPartition, minScore)

  override def matchesQuery[K, V](rdd: RDD[(K, V)],
                                  queryBuilder: V => Query,
                                  topK: Int = 10,
                                  minScore: Double = 0
                                 )
                                 (implicit kClassTag: ClassTag[K],
                                  vClassTag: ClassTag[V]): RDD[(K, (V, Array[SearchRecord[S]]))]=
    _searchRDD.matchesQuery(rdd, queryBuilder, topK, minScore)

  override def save(path: String): Unit = _searchRDD.save(path)

  override def getNumPartitions: Int = _searchRDD.getNumPartitions

  override def options: SearchOptions[S] = _searchRDD.options

  /**
   * Builds a search rdd with that custom search options.
   *
   * @param opts Search options
   * @return Dependent RDD with configurable search features
   */
  def searchRDD(opts: SearchOptions[S] = defaultOpts): SearchRDD[S] = new SearchRDDLucene[S](rdd, opts)

  override def searchJoinQuery[W: ClassTag](other: RDD[W],
                                            queryBuilder: W => Query,
                                            topKByPartition: Int,
                                            minScore: Double): RDD[(W, Option[SearchRecord[S]])] =
    _searchRDD.searchJoinQuery(other, queryBuilder, topKByPartition, minScore)

  override def distinct(numPartitions: Int): RDD[S] =
    _searchRDD.distinct(numPartitions)

  override def searchDropDuplicates[K: ClassTag, C: ClassTag](queryBuilder: S => Query = null,
                                                              createKey: S => K = (s: S) => s.hashCode.toLong.asInstanceOf[K],
                                                              minScore: Double = 0,
                                                              createCombiner: Seq[SearchRecord[S]] => C = (ss: Seq[SearchRecord[S]]) => ss.head.source.asInstanceOf[C],
                                                              mergeValue: (C, Seq[SearchRecord[S]]) => C = (c: C, _: Seq[SearchRecord[S]]) => c,
                                                              mergeCombiners: (C, C) => C = (c: C, _: C) => c,
                                                              numPartitionInJoin: Int = getNumPartitions,
                                                              topKToDeduplicate: Int = defaultTopK
                                                             )
                                                             (implicit ord: Ordering[K]): RDD[C] =
    _searchRDD.searchDropDuplicates(queryBuilder, createKey, minScore, createCombiner, mergeValue, mergeCombiners)

  private[spark] override def elementClassTag: ClassTag[S] = _searchRDD.elementClassTag
}
