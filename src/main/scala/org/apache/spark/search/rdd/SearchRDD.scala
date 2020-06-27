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

import java.io.{IOException, ObjectOutputStream}
import java.util.Objects

import org.apache.lucene.search.Query
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.search._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.Utils

import scala.reflect.ClassTag

/**
 * A search RDD indexes parent RDD partitions to lucene indexes.
 * It builds for all parent RDD partitions a one-2-one volatile Lucene index
 * available during the lifecycle of the spark session across executors local directories and RAM.
 *
 * @author Pierrick HYMBERT
 */
private[search] class SearchRDD[T: ClassTag](rdd: RDD[T],
                                             val options: SearchOptions[T])
  extends RDD[T](rdd.context, Nil) {

  private var searchIndexRDD = new SearchIndexedRDD(rdd, options).cache
  override val partitioner: Option[Partitioner] = searchIndexRDD.partitioner


  override def getPreferredLocations(split: Partition): Seq[String] =
    firstParent[T].asInstanceOf[SearchIndexedRDD[T]]
      .getPreferredLocations(split.asInstanceOf[SearchPartition[T]].searchIndexPartition)

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
    val indexDirectoryByPartition = searchIndexRDD._indexDirectoryByPartition
    searchIndexRDD.mapPartitionsWithIndex((index, _) =>
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
    val topKReducer = (matches: Iterator[SearchRecord[T]]) => matches.toArray
      .sortBy(_.score)(Ordering.Double.reverse)
      .take(topK)
      .iterator

    val otherZipped = other.zipWithIndex.map(_.swap)
    otherZipped
      .join(new MatchRDD[S, T](this, otherZipped, queryBuilder, topK, minScore))
      .reduceByKey((d1, d2) => (d1._1, topKReducer(d1._2 ++ d2._2)))
      .map {
        case (_, (doc, matches)) =>
          new Match[S, T](doc, matches.toArray)
      }
  }

  /**
   * alias for dropDuplicates
   */
  override def distinct(numPartitions: Int)(implicit ord: Ordering[T]): RDD[T] =
    searchDropDuplicates(numPartitions = numPartitions)

  /**
   * Drop duplicates records by applying lookup for matching hits of the query against this RDD.
   */
  def searchDropDuplicates(queryBuilder: T => Query = defaultQueryBuilder(),
                           topK: Int = Int.MaxValue,
                           minScore: Double = 0,
                           numPartitions: Int = getNumPartitions): RDD[T] = {
    searchQueryJoin[T](rdd, queryBuilder, topK, minScore) // FIXME add tests
      .map(m => {
        val matchHashes = m.hits.filter(_.hashCode != m.hashCode).map(_.source.hashCode)
        val allHashes = (Seq(m.hashCode) ++ matchHashes).sorted
        (Objects.hash(allHashes), m)
      })
      .reduceByKey((m1, _) => m1)
      .map(_._2.doc)
      .repartition(numPartitions)
  }

  override def repartition(numPartitions: Int)(implicit ord: Ordering[T]): RDD[T]
  = new SearchRDD[T](firstParent.repartition(numPartitions), options)

  def _partitionReaderSearchList(r: SearchPartitionReader[T],
                                 query: () => Query, topK: Int, minScore: Double): Array[SearchRecord[T]] =
    r.search(query(), topK, minScore).map(searchRecordJavaToProduct)

  protected[rdd] def reduceSearchRecordsByTopK(topK: Int): Iterator[Array[SearchRecord[T]]] => Array[SearchRecord[T]] =
    _.reduce(_ ++ _).sortBy(_.score)(Ordering[Double].reverse).take(topK)

  protected[rdd] def runSearchJob[R: ClassTag, A: ClassTag](searchByPartition: SearchPartitionReader[T] => R,
                                                            reducer: Iterator[R] => A): A =
    runSearchJobWithContext((_searchByPartition, _) => searchByPartition(_searchByPartition), reducer)

  protected[rdd] def runSearchJobWithContext[R: ClassTag, A: ClassTag](searchByPartitionWithContext: (SearchPartitionReader[T], TaskContext) => R,
                                                                       reducer: Iterator[R] => A): A = {
    val indexDirectoryByPartition = searchIndexRDD._indexDirectoryByPartition
    val ret = sparkContext.runJob(searchIndexRDD, (context: TaskContext, _: Iterator[T]) => {
      val index = context.partitionId()
      tryAndClose(reader(indexDirectoryByPartition, index)) {
        r => searchByPartitionWithContext(r, context)
      }
    })
    reducer(ret.toIterator)
  }

  def reader(indexDirectoryByPartition: Map[Int, String], index: Int): SearchPartitionReader[T] =
    reader(index, indexDirectoryByPartition(index))

  def reader(index: Int, indexDirectory: String): SearchPartitionReader[T] =
    new SearchPartitionReader[T](index, indexDirectory,
      elementClassTag.runtimeClass.asInstanceOf[Class[T]],
      options.getReaderOptions)

  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    val searchRDDPartition = split.asInstanceOf[SearchPartition[T]]

    // Trigger indexation if not done yet
    firstParent.iterator(searchRDDPartition.searchIndexPartition, context)
  }

  override protected def getPartitions: Array[Partition] = {
    // One-2-One partition
    firstParent.partitions.map(p =>
      new SearchPartition(p.index, searchIndexRDD)).toArray
  }

  override def persist(newLevel: StorageLevel): SearchRDD.this.type = {
    if (newLevel != StorageLevel.MEMORY_ONLY && newLevel != StorageLevel.NONE) {
      throw new SearchException("persisting SearchRDD is not supported, saveAsZip(String) to restore it later on")
    }
    super.persist(newLevel)
  }

  override def getDependencies: Seq[Dependency[_]] = Seq(new OneToOneDependency(searchIndexRDD))

  override def clearDependencies() {
    super.clearDependencies()
    searchIndexRDD.unpersist()
    searchIndexRDD = null
  }
}

class SearchPartition[T](val idx: Int,
                         @transient private val searchRDD: SearchIndexedRDD[T]) extends Partition {
  override def index: Int = idx

  var searchIndexPartition: SearchPartitionIndex[T] = searchRDD.partitions(idx).asInstanceOf[SearchPartitionIndex[T]]

  @throws(classOf[IOException])
  private def writeObject(oos: ObjectOutputStream): Unit = Utils.tryOrIOException {
    // Update the reference to parent split at the time of task serialization
    searchIndexPartition = searchRDD.partitions(idx).asInstanceOf[SearchPartitionIndex[T]]
    oos.defaultWriteObject()
  }
}
