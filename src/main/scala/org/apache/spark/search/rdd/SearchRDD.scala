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
import org.apache.spark.search.SearchException
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{OneToOneDependency, Partition, TaskContext}

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

/**
 * A search RDD indexes parent RDD partitions to lucene indexes.
 * It builds for all parent RDD partitions a one-2-one volatile Lucene index
 * available during the lifecycle of the spark session across executors local directories and RAM.
 *
 * @author Pierrick HYMBERT
 */
private[search] class SearchRDD[T: ClassTag](rdd: RDD[T],
                                             val options: SearchRDDOptions[T])
  extends RDD[T](rdd.context, Seq(new OneToOneDependency(rdd))) {

  /**
   * Return the number of indexed elements in the RDD.
   */
  override def count: Long = runSearchJob[Long, Long](spr => spr.count(), _.sum)

  /**
   * Count how many documents match the given query.
   */
  def count(query: String): Long = countQuery(parseQueryString(query))

  /**
   * Count how many documents match the given query.
   */
  def countQuery(query: () => Query): Long = runSearchJob[Long, Long](spr => spr.count(query()), _.sum)

  /**
   * Finds the top topK hits for this query.
   *
   * @note this method should only be used if the topK is expected to be small, as
   *       all the data is loaded into the driver's memory.
   */
  def searchList(query: String, topK: Int = Int.MaxValue, minScore: Double = 0): Array[SearchRecord[T]] =
    searchQueryList(parseQueryString(query), topK, minScore)

  /**
   * Finds the top topK hits for this query.
   *
   * @note this method should only be used if the topK is expected to be small, as
   *       all the data is loaded into the driver's memory.
   */
  def searchQueryList(query: () => Query, topK: Int = Int.MaxValue, minScore: Double = 0): Array[SearchRecord[T]] =
    runSearchJob[Array[SearchRecord[T]], Array[SearchRecord[T]]](
      spr => _partitionReaderSearchList(spr, query, topK, minScore).toArray,
      reduceSearchRecordsByTopK(topK))

  /**
   * Finds the top topK hits per partition for query.
   */
  def search(query: String, topKByPartition: Int = Int.MaxValue, minScore: Double = 0): RDD[SearchRecord[T]] =
    searchQuery(parseQueryString(query), topKByPartition, minScore)

  /**
   * Finds the top topK hits per partition for query.
   */
  def searchQuery(query: () => Query, topKByPartition: Int = Int.MaxValue, minScore: Double = 0): RDD[SearchRecord[T]] = {
    val indexDirectoryByPartition = _indexDirectoryByPartition
    mapPartitionsWithIndex((index, _) =>
      tryAndClose(reader(indexDirectoryByPartition, index)) {
        spr => _partitionReaderSearchList(spr, query, topKByPartition, minScore)
      }.iterator
    ).sortBy(_.score, ascending = false)
  }

  /**
   * Joins the input RDD against this one and returns matching hits.
   */
  def searchJoin[S: ClassTag](rdd: RDD[S],
                              queryBuilder: S => String,
                              topK: Int = Int.MaxValue,
                              minScore: Double = 0): RDD[Match[S, T]] =
    searchQueryJoin(rdd, queryStringBuilder(queryBuilder), topK, minScore)

  /**
   * Joins the input RDD against this one and returns matching hits.
   */
  def searchQueryJoin[S: ClassTag](other: RDD[S],
                                   queryBuilder: S => Query,
                                   topK: Int = Int.MaxValue,
                                   minScore: Double = 0): RDD[Match[S, T]] = {

    other.mapPartitionsWithIndex((index: Int, part: Iterator[S]) => part
      .zipWithIndex
      .map(_.swap)
      .map(doc => (index.toLong * other.getNumPartitions + doc._1, doc._2)))
      .join(new MatchRDD[S, T](this, other, queryBuilder, topK, minScore))
      .reduceByKey((d1, d2) => {
        (d1._1, d1._2 ++ d2._2)
      }).map {
      case (_, (doc, matches)) => new Match[S, T](doc, matches.toList
        .sortBy(_.score)(Ordering.Double.reverse)
        .take(topK).toArray)
    }
  }

  /**
   * alias for dropDuplicates
   */
  override def distinct(numPartitions: Int)(implicit ord: Ordering[T]): RDD[T] =
    dropDuplicates(numPartitions = numPartitions)

  /**
   * Drop duplicates records by applying lookup for matching hits of the query against this RDD.
   */
  def dropDuplicates(queryBuilder: T => Query = defaultQueryBuilder(),
                     minScore: Int = 0,
                     numPartitions: Int = getNumPartitions): RDD[T] = {
    // FIXME optimize
    searchQueryJoin[T](rdd, queryBuilder, minScore)
      .map(m => (m.hits.map(_.source.hashCode).mkString("-"), m))
      .reduceByKey((m1, _) => m1)
      .map(_._2.doc)
  }

  override def repartition(numPartitions: Int)(implicit ord: Ordering[T]): RDD[T]
  = new SearchRDD[T](firstParent.repartition(numPartitions), options)

  private def _partitionReaderSearchList(r: SearchPartitionReader[T],
                                         query: () => Query, topK: Int, minScore: Double): Array[SearchRecord[T]] =
    r.search(query(), topK, minScore).map(searchRecordJavaToProduct)

  protected def reduceSearchRecordsByTopK(topK: Int): Iterator[Array[SearchRecord[T]]] => Array[SearchRecord[T]] =
    _.reduce(_ ++ _).sortBy(_.score)(Ordering[Double].reverse).take(topK)

  protected def runSearchJob[R: ClassTag, A: ClassTag](searchByPartition: SearchPartitionReader[T] => R,
                                                       reducer: Iterator[R] => A): A =
    runSearchJobWithContext((_searchByPartition, _) => searchByPartition(_searchByPartition), reducer)

  protected def runSearchJobWithContext[R: ClassTag, A: ClassTag](searchByPartitionWithContext: (SearchPartitionReader[T], TaskContext) => R,
                                                                  reducer: Iterator[R] => A): A = {
    val indexDirectoryByPartition = _indexDirectoryByPartition
    val ret = sparkContext.runJob(this, (context: TaskContext, _: Iterator[T]) => {
      val index = context.partitionId()
      tryAndClose(reader(indexDirectoryByPartition, index)) {
        r => searchByPartitionWithContext(r, context)
      }
    })
    reducer(ret.toIterator)
  }

  private def reader(indexDirectoryByPartition: Map[Int, String], index: Int): SearchPartitionReader[T] =
    reader(index, indexDirectoryByPartition(index))

  private[rdd] def reader(index: Int, indexDirectory: String): SearchPartitionReader[T] =
    new SearchPartitionReader[T](index, indexDirectory,
      elementClassTag.runtimeClass.asInstanceOf[Class[T]],
      options.getReaderOptions)

  private[rdd] lazy val _indexDirectoryByPartition =
    partitions.map(_.asInstanceOf[SearchPartition[T]]).zipWithIndex.map(t => (t._2, t._1.indexDir)).toMap

  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    val searchRDDPartition = split.asInstanceOf[SearchPartition[T]]

    val elements = firstParent.iterator(searchRDDPartition.parent, context).asJava
      .asInstanceOf[java.util.Iterator[T]]
    searchRDDPartition.index(elements, options.getIndexationOptions)

    Iterator.empty // No RDD expected after
  }

  override protected def getPartitions: Array[Partition] = {
    // One-2-One partition
    firstParent.partitions.map(p =>
      new SearchPartition[T](p.index,
        s"${options.getIndexationOptions.getRootIndexDirectory}-rdd${id}", p)).toArray
  }

  override protected[rdd] def getPreferredLocations(split: Partition): Seq[String] = {
    // Try to balance partitions across executors
    val allIds = context.getExecutorIds()
    val ids = allIds.sliding(getNumPartitions).toList
    if (split.index < ids.size) {
      ids(split.index)
    } else {
      super.getPreferredLocations(split)
    }
  }

  override def persist(newLevel: StorageLevel): SearchRDD.this.type = {
    if (newLevel != StorageLevel.MEMORY_ONLY && newLevel != StorageLevel.NONE) {
      throw new SearchException("persisting SearchRDD is not supported, saveAsZip(String) to restore it later on")
    }
    super.persist(newLevel)
  }
}

