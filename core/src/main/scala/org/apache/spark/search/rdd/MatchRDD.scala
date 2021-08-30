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
import org.apache.spark.search.{SearchException, SearchRecord}
import org.apache.spark.util.Utils

import scala.reflect.ClassTag

/**
 * Result RDD of a
 * [[org.apache.spark.search.rdd.SearchRDD#searchJoinQuery(org.apache.spark.rdd.RDD, scala.Function1, int, double, scala.reflect.ClassTag)]]
 *
 * @author Pierrick HYMBERT
 */
class MatchRDD[S: ClassTag, H: ClassTag](@transient var searchRDD: SearchRDDLucene[H],
                                         @transient var other: RDD[(Long, S)],
                                         queryBuilder: S => Query,
                                         topK: Int = Int.MaxValue,
                                         minScore: Double = 0)
  extends RDD[(Long, Array[SearchRecord[H]])](searchRDD.context, Nil)
    with Serializable {

  override val partitioner: Option[Partitioner] = searchRDD.partitioner

  override protected def getPreferredLocations(split: Partition): Seq[String] =
    firstParent[H].asInstanceOf[SearchRDDLucene[H]]
      .getPreferredLocations(split.asInstanceOf[MatchRDDPartition].searchPartition)

  override def compute(split: Partition, context: TaskContext): Iterator[(Long, Array[SearchRecord[H]])] = {
    val matchPartition = split.asInstanceOf[MatchRDDPartition]

    // Be sure partition is indexed in our worker
    val it = parent[Array[Byte]](1).iterator(matchPartition.searchPartition.searchIndexPartition, context)

    // Unzip if needed
    ZipUtils.unzipPartition(matchPartition.searchPartition.searchIndexPartition.indexDir, it)

    // Match other partitions against our
    tryAndClose(firstParent[H].asInstanceOf[SearchRDDLucene[H]].reader(matchPartition.searchPartition.index,
      matchPartition.searchPartition.searchIndexPartition.indexDir)) {
      spr =>
        matchPartition.otherPartitions.flatMap(op =>
          parent[(Long, S)](2).iterator(op, context)
            .map(docIndex => (docIndex._1,
              try {
                spr.search(queryBuilder(docIndex._2), topK, minScore).map(searchRecordJavaToProduct)
              } catch {
                case e: SearchException => throw new SearchException(s"error during matching ${docIndex._1}: ${docIndex._2}", e)
              })
            )).toList.toIterator
    }
  }

  override def clearDependencies(): Unit = {
    super.clearDependencies()
    searchRDD = null
    other = null
  }

  override protected def getPartitions: Array[Partition] = {
    val parts = new Array[Partition](searchRDD.partitions.length)
    for (s1 <- parent[H](0).partitions) {
      parts(s1.index) = new MatchRDDPartition(s1.index, parent[S](2).partitions.map(_.index), searchRDD, other)
    }
    parts
  }

  override def getDependencies: Seq[Dependency[_]] = Seq(
    new OneToOneDependency(searchRDD),
    new OneToOneDependency(searchRDD.indexerRDD),
    new OneToOneDependency(other)
  )

  class MatchRDDPartition(val idx: Int,
                          val otherIndex: Array[Int],
                          @transient private val searchRDD: SearchRDDLucene[H],
                          @transient private val other: RDD[(Long, S)]) extends Partition {
    override def index: Int = idx

    var searchPartition: SearchPartition[H] = searchRDD.partitions(idx).asInstanceOf[SearchPartition[H]]
    var otherPartitions: Array[Partition] = otherIndex.map(i => other.partitions(i))

    @throws(classOf[IOException])
    private def writeObject(oos: ObjectOutputStream): Unit = Utils.tryOrIOException {
      // Update the reference to parent split at the time of task serialization
      searchPartition = searchRDD.partitions(idx).asInstanceOf[SearchPartition[H]]
      otherPartitions = otherIndex.map(i => other.partitions(i))
      oos.defaultWriteObject()
    }
  }

}
