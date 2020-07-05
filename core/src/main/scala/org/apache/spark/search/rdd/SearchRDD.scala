/**
 * Copyright © 2020 Spark Search (The Spark Search Contributors)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
private[search] class SearchRDD[T: ClassTag](sc: SparkContext,
                                             var searchIndexRDD: SearchIndexedRDD[T],
                                             val options: SearchOptions[T],
                                             val deps: Seq[Dependency[_]])
  extends RDD[T](sc, Seq(new OneToOneDependency(searchIndexRDD.persist(StorageLevel.DISK_ONLY))) ++ deps) {

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
  def searchDropDuplicates(queryBuilder: T => Query = defaultQueryBuilder(options),
                           topK: Int = Int.MaxValue,
                           minScore: Double = 0,
                           numPartitions: Int = getNumPartitions): RDD[T] = {
    searchQueryJoin[T](parent(1), queryBuilder, topK, minScore) // FIXME add tests
      .map(m => {
        val matchHashes = m.hits.filter(_.source.hashCode != m.doc.hashCode).map(_.source.hashCode)
        val allHashes = (Seq(m.doc.hashCode) ++ matchHashes).sorted
        (Objects.hash(allHashes), m.doc)
      })
      .reduceByKey((m1, _) => m1)
      .map(_._2)
      .repartition(numPartitions)
  }

  /**
   * Save the current indexed RDD in order to be able to reload it later.
   *
   * @param path Path on the spark file system to save on
   */
  def save(path: String): Unit = {
    logInfo(s"Saving index with ${getNumPartitions} partitions to ${path} ...")
    // Be sure we are indexed
    count

    searchIndexRDD.save(path)

    logInfo(s"Index with ${getNumPartitions} partitions saved to ${path}")
  }

  def this(rdd: RDD[T], options: SearchOptions[T]) {
    this(rdd.sparkContext, new SearchIndexedRDD(rdd, options), options, Seq(new OneToOneDependency(rdd)))
  }

  override val partitioner: Option[Partitioner] = searchIndexRDD.partitioner

  override def getPreferredLocations(split: Partition): Seq[String] =
    firstParent[T].asInstanceOf[SearchIndexedRDD[T]]
      .getPreferredLocations(split.asInstanceOf[SearchPartition[T]].searchIndexPartition)

  override def repartition(numPartitions: Int)(implicit ord: Ordering[T]): RDD[T]
  = new SearchRDD[T](firstParent.firstParent.repartition(numPartitions), options)

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
    val ret = sparkContext.runJob(this, (context: TaskContext, partitionZipped: Iterator[T]) => {
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
    val it = firstParent[Array[Byte]].iterator(searchRDDPartition.searchIndexPartition, context)

    // Unzip if needed
    ZipUtils.unzipPartition(searchRDDPartition.searchIndexPartition.indexDir, it)

    Iterator()
  }

  override protected def getPartitions: Array[Partition] = {
    // One-2-One partition
    firstParent.partitions.map(p =>
      new SearchPartition(p.index, searchIndexRDD)).toArray
  }

  override def persist(newLevel: StorageLevel): SearchRDD.this.type = {
    if (newLevel != StorageLevel.MEMORY_ONLY && newLevel != StorageLevel.NONE) {
      throw new SearchException("persisting SearchRDD is not supported, save(String) to restore it later on")
    }
    super.persist(newLevel)
  }

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
  def load[T: ClassTag](sc: SparkContext, path: String, options: SearchOptions[T] = defaultOpts[T]): SearchRDD[T] =
    new SearchRDD[T](sc, new SearchIndexReloadedRDD[T](sc, path, options), options, Nil)
}
