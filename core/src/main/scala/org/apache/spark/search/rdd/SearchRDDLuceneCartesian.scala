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

import org.apache.lucene.search.Query
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.search.{ReaderOptions, SearchException, SearchRecord}
import org.apache.spark.util.Utils

import scala.reflect.{ClassTag, classTag}

/**
 * Result RDD of a cartesian search.
 *
 * @author Pierrick HYMBERT
 */
class SearchRDDLuceneCartesian[S: ClassTag, H: ClassTag](
                                                          @transient var searchRDDLuceneIndexer: SearchRDDLuceneIndexer[H],
                                                          @transient var other: RDD[S],
                                                          queryBuilder: S => Query,
                                                          readerOptions: ReaderOptions[H],
                                                          topK: Int = Int.MaxValue,
                                                          minScore: Double = 0)
  extends RDD[(S, SearchRecord[H])](searchRDDLuceneIndexer.context, Nil)
    with Serializable {

  override val partitioner: Option[Partitioner] = searchRDDLuceneIndexer.partitioner

  override protected def getPreferredLocations(split: Partition): Seq[String] =
    firstParent[H].asInstanceOf[SearchRDDLuceneIndexer[H]]
      .getPreferredLocations(split.asInstanceOf[MatchRDDPartition].searchIndexPartition)

  override def compute(split: Partition, context: TaskContext): Iterator[(S, SearchRecord[H])] = {
    val matchPartition = split.asInstanceOf[MatchRDDPartition]

    // Be sure partition is indexed in our worker
    val it = firstParent[Array[Byte]].iterator(matchPartition.searchIndexPartition, context)

    // Unzip if needed
    ZipUtils.unzipPartition(matchPartition.searchIndexPartition.indexDir, it)

    // Match other partition against our one
    tryAndClose(reader(matchPartition.searchIndexPartition.index,
      matchPartition.searchIndexPartition.indexDir)) {
      spr =>
        parent[S](1).iterator(matchPartition.otherPartition, context)
          .flatMap(searchFor => {
            try {
              spr.search(queryBuilder(searchFor), topK, minScore)
                .map(searchRecordJavaToProduct)
                .map(s => (searchFor, s))
            } catch {
              case e: SearchException => throw new SearchException(s"error during matching $searchFor: $e", e)
            }
          }).toList.toIterator // Force search to be computed within the reader resource
    }
  }

  override def clearDependencies(): Unit = {
    super.clearDependencies()
    searchRDDLuceneIndexer = null
    other = null
  }

  private val numPartitionsInOtherRdd = other.partitions.length

  override protected def getPartitions: Array[Partition] = {
    val parts = new Array[Partition](searchRDDLuceneIndexer.partitions.length * numPartitionsInOtherRdd)
    for (s1 <- searchRDDLuceneIndexer.partitions; s2 <- other.partitions) {
      val idx = s1.index * numPartitionsInOtherRdd + s2.index
      parts(idx) = new MatchRDDPartition(idx, searchRDDLuceneIndexer, other, s1.index, s2.index)
    }
    parts
  }

  override def getDependencies: Seq[Dependency[_]] = List(
    new NarrowDependency(searchRDDLuceneIndexer) {
      def getParents(id: Int): Seq[Int] = List(id / numPartitionsInOtherRdd)
    },
    new NarrowDependency(other) {
      def getParents(id: Int): Seq[Int] = List(id % numPartitionsInOtherRdd)
    }
  )

  private def reader(index: Int, indexDirectory: String): SearchPartitionReader[H] =
    new SearchPartitionReader[H](index, indexDirectory, classTag[H].runtimeClass.asInstanceOf[Class[H]],
      readerOptions)

  class MatchRDDPartition(val idx: Int,
                          @transient private val searchRDDLuceneIndexer: SearchRDDLuceneIndexer[H],
                          @transient private val other: RDD[S],
                          val searchRDDIndex: Int,
                          val otherIndex: Int
                         ) extends Partition {
    override val index: Int = idx

    var searchIndexPartition: SearchPartitionIndex[H] = searchRDDLuceneIndexer.partitions(searchRDDIndex).asInstanceOf[SearchPartitionIndex[H]]
    var otherPartition: Partition = other.partitions(otherIndex)

    @throws(classOf[IOException])
    private def writeObject(oos: ObjectOutputStream): Unit = Utils.tryOrIOException {
      // Update the reference to parent split at the time of task serialization
      searchIndexPartition = searchRDDLuceneIndexer.partitions(searchRDDIndex).asInstanceOf[SearchPartitionIndex[H]]
      otherPartition = other.partitions(otherIndex)
      oos.defaultWriteObject()
    }
  }

}
