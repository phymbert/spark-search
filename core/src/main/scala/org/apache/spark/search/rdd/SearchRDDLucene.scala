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
import java.util.Objects

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.lucene.search.Query
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.search._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.{BoundedPriorityQueue, Utils}

import scala.reflect.ClassTag


/**
 * A search RDD indexes parent RDD partitions to lucene indexes.
 * It builds for all parent RDD partitions a one-2-one volatile Lucene index
 * available during the lifecycle of the spark session across executors local directories and RAM.
 *
 * @author Pierrick HYMBERT
 */
private[search] class SearchRDDLucene[S: ClassTag](sc: SparkContext,
                                                   val indexerRDD: SearchRDDLuceneIndexer[S],
                                                   val options: SearchOptions[S],
                                                   val deps: Seq[Dependency[_]])
  extends RDD[S](sc, Seq(new OneToOneDependency(indexerRDD)) ++ deps)
    with SearchRDD[S] {

  def this(rdd: RDD[S], options: SearchOptions[S]) {
    this(rdd.sparkContext,
      new SearchRDDLuceneIndexer(rdd, options),
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
                               topK: Int = Int.MaxValue,
                               minScore: Double = 0
                              ): Array[SearchRecord[S]] =
    runSearchJob[Array[SearchRecord[S]], Array[SearchRecord[S]]](
      spr => _partitionReaderSearchList(spr, query(), topK, minScore),
      reduceSearchRecordsByTopK(topK))

  override def searchQuery(query: StaticQueryProvider,
                           topKByPartition: Int = Int.MaxValue,
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

  override def searchJoinQuery[K: ClassTag, V: ClassTag](other: RDD[(K, V)],
                                                         queryBuilder: V => Query,
                                                         topKByPartition: Int = Int.MaxValue,
                                                         minScore: Double = 0): RDD[(K, (V, SearchRecord[S]))] = {
    new SearchRDDLuceneCartesian[K, V, S](
      indexerRDD, other, queryBuilder,
      options.getReaderOptions, topKByPartition, minScore
    )
  }

  override def matchesQuery[K: ClassTag, V: ClassTag](other: RDD[(K, V)],
                                                      queryBuilder: V => Query,
                                                      topK: Int = 10,
                                                      minScore: Double = 0
                                                     ): RDD[DocAndHits[V, S]] = {

    val cartesianRDD: RDD[(K, (V, SearchRecord[S]))] =
      new SearchRDDLuceneCartesian[K, V, S](
        indexerRDD, other, queryBuilder,
        options.getReaderOptions, topK, minScore
      ).map {
        case (id: K, (v: V, sr: SearchRecord[S])) => (id, (v, sr))
      }

    val ord: Ordering[(V, SearchRecord[S])] = Ordering.by(_._2.score)

    cartesianRDD
      .aggregateByKey(new BoundedPriorityQueue[(V, SearchRecord[S])](topK)(ord.reverse))(
        seqOp = (queue, item) => queue += item,
        combOp = (queue1, queue2) => queue1 ++= queue2
      ).mapValues(_.toList)
      .map {
        case (_, docAndHits :: tail) =>
          // FIXME historically no key, need to add it to keep a PairRDD
          new DocAndHits[V, S](docAndHits._1, Array(docAndHits._2) ++ tail.map(_._2))
      }
  }

  override def distinct(numPartitions: Int): RDD[S] =
    searchDropDuplicates(numPartitions = numPartitions)

  override def searchDropDuplicates(queryBuilder: S => Query = defaultQueryBuilder(options),
                                    topK: Int = 10,
                                    minScore: Double = 0,
                                    numPartitions: Int = getNumPartitions
                                   ): RDD[S] = {
    val zipped: RDD[(Long, S)] = parent[S](1).zipWithIndex().map(_.swap)
    val matched = matchesQuery[Long, S](zipped, queryBuilder, topK, minScore)
    // FIXME #86 SearchRDD#searchDropDuplicates should take an accumulator func to deal with duplicates
    matched.map(m => {
      val matchHashes = m.hits.filter(_.source.hashCode != m.doc.hashCode).map(_.source.hashCode)
      val allHashes = (Seq(m.doc.hashCode) ++ matchHashes).sorted
      (Objects.hash(allHashes), m.doc)
    })
      .reduceByKey((m1, _) => m1)
      .map(_._2)
      .repartition(numPartitions)
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

  override val partitioner: Option[Partitioner] = indexerRDD.partitioner

  override def getPreferredLocations(split: Partition): Seq[String] =
    firstParent[S].asInstanceOf[SearchRDDLuceneIndexer[S]]
      .getPreferredLocations(split.asInstanceOf[SearchPartition[S]].searchIndexPartition)

  override def repartition(numPartitions: Int)(implicit ord: Ordering[S]): RDD[S]
  = new SearchRDDLucene[S](firstParent.firstParent.repartition(numPartitions), options)

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

    val indexedRDD = firstParent[Array[Byte]].asInstanceOf[SearchRDDLuceneIndexer[S]]

    // Trigger indexation if not done yet on parent rdd partition node
    val it: Iterator[Array[Byte]] = indexedRDD.iterator(partition.searchIndexPartition, context)

    val indexDirectory = partition.searchIndexPartition.indexDir

    // Unzip if needed
    ZipUtils.unzipPartition(indexDirectory, it)

    tryAndClose(reader(partition.index, indexDirectory)) {
      r => r.allDocs().map(searchRecordJavaToProduct).map(_.source)
    }.iterator
  }

  override protected def getPartitions: Array[Partition] = {
    // One-2-One partition
    firstParent.partitions.map(p =>
      new SearchPartition(p.index, indexerRDD)).toArray
  }

  override def persist(newLevel: StorageLevel): SearchRDDLucene.this.type = {
    if (newLevel != StorageLevel.MEMORY_ONLY && newLevel != StorageLevel.NONE) {
      throw new SearchException("persisting SearchRDD is not supported, save(String) to restore it later on")
    }
    super.persist(newLevel)
  }

  override def clearDependencies(): Unit = {
    super.clearDependencies()
    if (options.getIndexationOptions.isCacheSearchIndexRDD) {
      indexerRDD.unpersist()
    }
  }
}

class SearchPartition[T](val idx: Int,
                         @transient private val searchRDD: SearchRDDLuceneIndexer[T]) extends Partition {
  override def index: Int = idx

  var searchIndexPartition: SearchPartitionIndex[T] = searchRDD.partitions(idx).asInstanceOf[SearchPartitionIndex[T]]

  @throws(classOf[IOException])
  private def writeObject(oos: ObjectOutputStream): Unit = Utils.tryOrIOException {
    // Update the reference to parent split at the time of task serialization
    searchIndexPartition = searchRDD.partitions(idx).asInstanceOf[SearchPartitionIndex[T]]
    oos.defaultWriteObject()
  }
}