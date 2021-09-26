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

import java.io.{IOException, ObjectOutputStream}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.lucene.search.Query
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.search._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.{BoundedPriorityQueue, Utils}

import scala.collection.JavaConverters.asScalaIteratorConverter
import scala.reflect.ClassTag


/**
 * A search RDD indexes parent RDD partitions to lucene indexes.
 * It builds for all parent RDD partitions a one-2-one volatile Lucene index
 * available during the lifecycle of the spark session across executors local directories and RAM.
 *
 * @author Pierrick HYMBERT
 */
private[search] class SearchRDDImpl[S: ClassTag](sc: SparkContext,
                                                 val indexerRDD: SearchRDDIndexer[S],
                                                 val options: SearchOptions[S],
                                                 val deps: Seq[Dependency[_]])
  extends RDD[S](sc, Seq(new OneToOneDependency(indexerRDD)) ++ deps)
    with SearchRDD[S] {

  def this(rdd: RDD[S], options: SearchOptions[S]) {
    this(rdd.sparkContext,
      new SearchRDDIndexer(rdd, options),
      options,
      Seq(new OneToOneDependency(rdd)))
  }

  if (options.getIndexationOptions.isCacheSearchIndexRDD) {
    indexerRDD.persist(StorageLevel.DISK_ONLY)
  }

  override def count(): Long = runSearchJob[Long, Long](spr => spr.count(), _.sum)

  override def count(query: StaticQueryProvider): Long =
    runSearchJob[Long, Long](spr => spr.count(query()), _.sum)

  override def searchListQuery(query: StaticQueryProvider,
                               topK: Int = defaultTopK,
                               minScore: Double = 0
                              ): Array[SearchRecord[S]] =
    runSearchJob[Array[SearchRecord[S]], Array[SearchRecord[S]]](
      spr => _partitionReaderSearchList(spr, query(), topK, minScore),
      reduceSearchRecordsByTopK(topK))

  override def searchQuery(query: StaticQueryProvider,

                           topKByPartition: Int = defaultTopK,
                           minScore: Double = 0
                          ): RDD[SearchRecord[S]] = {
    val indexDirectoryByPartition = indexerRDD._indexDirectoryByPartition
    indexerRDD.mapPartitionsWithIndex(
      (index, _) =>
        tryAndClose(reader(indexDirectoryByPartition, index)) {
          spr => _partitionReaderSearchList(spr, query(), topKByPartition, minScore)
        }.iterator
    ).sortBy(_.score, ascending = false)
  }

  override def matchesQuery[K, V](other: RDD[(K, V)],
                                  queryBuilder: V => Query,
                                  topK: Int = 10,
                                  minScore: Double = 0
                                 )
                                 (implicit kClassTag: ClassTag[K],
                                  vClassTag: ClassTag[V]): RDD[(K, (V, Array[SearchRecord[S]]))] = {
    val unwrapDoc = sparkContext.clean((kv: (K, V)) => queryBuilder(kv._2))

    val cartesianRDD: RDD[((K, V), Option[SearchRecord[S]])] =
      new SearchRDDCartesian[(K, V), S](
        indexerRDD, other, unwrapDoc,
        options.getReaderOptions, topK, minScore
      )

    val pairedRDD: RDD[(K, (V, Option[SearchRecord[S]]))] = cartesianRDD.map {
      case ((k: K, v: V), Some(sr)) => (k, (v, Some(sr)))
      case ((k: K, v: V), None) => (k, (v, None))
    }

    // TopK monoid
    implicit val ord: Ordering[(V, Option[SearchRecord[S]])] = Ordering.by(_._2.map(_.score))
    val topKByKey = pairedRDD
      .aggregateByKey(new BoundedPriorityQueue[(V, Option[SearchRecord[S]])](topK)(ord))(
        seqOp = (topK, searchRecord) => topK += searchRecord,
        combOp = (topK1, topK2) => topK1 ++= topK2
      )

    val matchesByKey = topKByKey
      .mapValues(matches => (matches.head._1, matches.filter(_._2.isDefined).map(_._2.get).toArray.reverse))
    matchesByKey
  }

  override def searchJoinQuery[W: ClassTag](other: RDD[W],
                                            queryBuilder: W => Query,
                                            topKByPartition: Int = defaultTopK,
                                            minScore: Double = 0): RDD[(W, Option[SearchRecord[S]])] = {
    new SearchRDDCartesian[W, S](
      indexerRDD,
      other, queryBuilder,
      options.getReaderOptions, topKByPartition, minScore
    )
  }

  /**
   * Alias for
   * [[org.apache.spark.search.rdd.SearchRDD#searchDropDuplicates(scala.Function1, int, double, int)}]]
   */
  override def distinct(numPartitions: Int): RDD[S] =
    searchDropDuplicates[Long, S]()

  /**
   * Drops duplicated records by applying lookup for matching hits of the query against this RDD.
   *
   * @param queryBuilder builds the lucene query to search for duplicate
   * @param minScore     minimum score of matching documents
   */
  override def searchDropDuplicates[K: ClassTag, C: ClassTag](
                                                               queryBuilder: S => Query = null, // Default query builder
                                                               createKey: S => K = (s: S) => s.hashCode.toLong.asInstanceOf[K],
                                                               minScore: Double = 0,
                                                               createCombiner: Seq[SearchRecord[S]] => C = (ss: Seq[SearchRecord[S]]) => ss.head.source.asInstanceOf[C],
                                                               mergeValue: (C, Seq[SearchRecord[S]]) => C = (c: C, _: Seq[SearchRecord[S]]) => c,
                                                               mergeCombiners: (C, C) => C = (c: C, _: C) => c,
                                                               numPartitionInJoin: Int = getNumPartitions,
                                                               topKToDeduplicate: Int = defaultTopK
                                                             )(implicit ord: Ordering[K]): RDD[C] = {
    val cleanedKey = sparkContext.clean(createKey)
    val unwrapDoc = sparkContext.clean((ks: (K, S)) => (queryBuilder match {
      case null => defaultQueryBuilder[S](options)(elementClassTag)
      case _ => queryBuilder
    }) (ks._2))
    val pairedRDD = map(s => (cleanedKey(s), s)).repartition(numPartitionInJoin)

    val cartesianRDD: RDD[((K, S), Option[SearchRecord[S]])] =
      new SearchRDDCartesian[(K, S), S](
        indexerRDD, pairedRDD,
        unwrapDoc,
        options.getReaderOptions,
        topKToDeduplicate,
        minScore)

    val pairedWithSearchedRDD: RDD[(K, (S, Option[SearchRecord[S]]))] = cartesianRDD.map {
      case ((k: K, s: S), Some(sr)) => (k, (s, Some(sr)))
      case ((k: K, s: S), None) => (k, (s, None))
    }

    val hitsByKey: RDD[(K, List[(S, Option[SearchRecord[S]])])] =
      pairedWithSearchedRDD.aggregateByKey(List[(S, Option[SearchRecord[S]])]())(
        seqOp = (matches, searchRecord) => {
          matches ++ List(searchRecord)
        },
        combOp = (matches1, matches2) => {
          matches1 ++ matches2
        }
      )

    val keysAndDocs: RDD[(Seq[K], Seq[SearchRecord[S]])] = hitsByKey.map {
      case (key: K, matches: List[(S, Option[SearchRecord[S]])]) =>
        val doc: S = matches.head._1 // assumed left join
        val otherMatches = matches
          .filter(_._2.isDefined)
          .map(m => (createKey(m._2.get.source), m._2.get))
          // Remove self matching if exists (depending on score filter)
          .filter((ks) => ks._1 != key)
        val keys = (Seq(key) ++ otherMatches.map(_._1)).sorted
        (keys, Seq(new SearchRecord[S](-1, -1, 0, -1, doc)) ++ otherMatches.map(_._2))
    }

    keysAndDocs.combineByKey(createCombiner, mergeValue, mergeCombiners)
      .values
  }

  override def save(pathString: String): Unit = {
    logInfo(s"Saving index with $getNumPartitions partitions to $pathString ...")
    // Be sure we are indexed
    count()

    val hadoopConf = new Configuration()
    val hdfs = FileSystem.get(hadoopConf)
    val path = new Path(pathString)
    if (hdfs.exists(path)) {
      // FIXME issue github #77 https://github.com/phymbert/spark-search/issues/77
      throw new SearchException(s"HDFS path $path already exists, delete it first")
    }

    indexerRDD.save(pathString)

    logInfo(s"Index with $getNumPartitions partitions saved to $path")
  }

  private[spark] override def elementClassTag: ClassTag[S] = super.elementClassTag

  override val partitioner: Option[Partitioner] = indexerRDD.partitioner

  override def getPreferredLocations(split: Partition): Seq[String] =
    firstParent[S].asInstanceOf[SearchRDDIndexer[S]]
      .getPreferredLocations(split.asInstanceOf[SearchPartition[S]].searchIndexPartition)

  override def repartition(numPartitions: Int)(implicit ord: Ordering[S]): RDD[S]
  = new SearchRDDImpl[S](firstParent.firstParent.repartition(numPartitions), options)

  def _partitionReaderSearchList(r: SearchPartitionReader[S],
                                 query: Query, topK: Int, minScore: Double): Array[SearchRecord[S]] =
    r.search(query, topK, minScore).map(searchRecordJavaToProduct)

  protected[rdd] def reduceSearchRecordsByTopK(topK: Int): Iterator[Array[SearchRecord[S]]] => Array[SearchRecord[S]] =
    _.reduce(_ ++ _).sortBy(_.score)(Ordering[Double].reverse).take(topK)

  protected[rdd] def runSearchJob[R: ClassTag, A: ClassTag](searchByPartition: SearchPartitionReader[S] => R,
                                                            reducer: Iterator[R] => A): A =
    runSearchJobWithContext((_searchByPartition, _) => searchByPartition(_searchByPartition), reducer)

  protected[rdd] def runSearchJobWithContext[R: ClassTag, A: ClassTag](searchByPartitionWithContext: (SearchPartitionReader[S], TaskContext) => R,
                                                                       reducer: Iterator[R] => A): A = {
    val indexDirectoryByPartition = indexerRDD._indexDirectoryByPartition
    val ret = sparkContext.runJob(indexerRDD, (context: TaskContext, it: Iterator[Array[Byte]]) => {
      val index = context.partitionId()

      // Unzip if needed
      ZipUtils.unzipPartition(indexDirectoryByPartition(index), it)

      tryAndClose(reader(indexDirectoryByPartition, index)) {
        r => searchByPartitionWithContext(r, context)
      }
    })
    reducer(ret.toIterator)
  }

  private def reader(indexDirectoryByPartition: Map[Int, String], index: Int): SearchPartitionReader[S] =
    reader(index, indexDirectoryByPartition(index))

  private def reader(index: Int, indexDirectory: String): SearchPartitionReader[S] =
    new SearchPartitionReader[S](index, indexDirectory,
      elementClassTag.runtimeClass.asInstanceOf[Class[S]],
      options.getReaderOptions)

  override def compute(split: Partition, context: TaskContext): Iterator[S] = {
    val partition = split.asInstanceOf[SearchPartition[S]]

    val indexedRDD = firstParent[Array[Byte]].asInstanceOf[SearchRDDIndexer[S]]

    // Trigger indexation if not done yet on parent rdd partition node
    val it: Iterator[Array[Byte]] = indexedRDD.iterator(partition.searchIndexPartition, context)

    val indexDirectory = partition.searchIndexPartition.indexDir

    // Unzip if needed
    ZipUtils.unzipPartition(indexDirectory, it)

    val spr = reader(partition.searchIndexPartition.index,
      partition.searchIndexPartition.indexDir)

    context.addTaskCompletionListener[Unit](ctx => {
      spr.close()
    })

    spr.docs().asScala.map(searchRecordJavaToProduct).map(_.source)
  }

  override protected def getPartitions: Array[Partition] = {
    // One-2-One partition
    firstParent.partitions.map(p =>
      new SearchPartition(p.index, indexerRDD)).toArray
  }

  override def clearDependencies(): Unit = {
    super.clearDependencies()
    if (options.getIndexationOptions.isCacheSearchIndexRDD) {
      indexerRDD.unpersist()
    }
  }
}

class SearchPartition[T](val idx: Int,
                         @transient private val searchRDD: SearchRDDIndexer[T]) extends Partition {
  override def index: Int = idx

  var searchIndexPartition: SearchPartitionIndex[T] = searchRDD.partitions(idx).asInstanceOf[SearchPartitionIndex[T]]

  @throws(classOf[IOException])
  private def writeObject(oos: ObjectOutputStream): Unit = Utils.tryOrIOException {
    // Update the reference to parent split at the time of task serialization
    searchIndexPartition = searchRDD.partitions(idx).asInstanceOf[SearchPartitionIndex[T]]
    oos.defaultWriteObject()
  }
}