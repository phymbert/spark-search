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

import org.apache.spark.{OneToOneDependency, Partition, TaskContext}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

import collection.JavaConverters._

/**
 * A search RDD indexes parent RDD partitions to lucene indexes.
 * Offers search features.
 *
 * Limitation: At the moment it cannot be neither persisted or restored or moved.
 *
 * @author Pierrick HYMBERT
 */
private[search] class SearchRDD[T: ClassTag](@transient val rdd: RDD[T],
                                             val options: SearchRDDOptions[T])
  extends RDD[T](rdd.sparkContext, Seq(new OneToOneDependency(rdd))) {

  override def count: Long = runSearchJob[Long, Long](searchPartitionReader => searchPartitionReader.count(), _.sum)

  private def runSearchJob[R: ClassTag, A: ClassTag](searchByPartition: (SearchPartitionReader[T]) => R,
                                                     reducer: (Iterator[R]) => A): A = {

    val ret = sparkContext.runJob(this, (context: TaskContext, _: Iterator[T]) => {
      val index = context.partitionId()
      val searchPartition = partitions(index).asInstanceOf[SearchPartition[T]]
      val searchPartitionReader = new SearchPartitionReader[T](index, searchPartition.indexDir, options.getReaderOptions)
      searchByPartition(searchPartitionReader)
    })
    reducer(ret.toIterator)
  }

  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    val searchRDDPartition = split.asInstanceOf[SearchPartition[T]]
    val parentRDD = firstParent

    val parentPartition = searchRDDPartition.parent
    val elements = parentRDD.iterator(parentPartition, context).asJava
      .asInstanceOf[java.util.Iterator[T]]
    searchRDDPartition.index(elements, options.getIndexationOptions)

    Iterator.empty // No RDD expected after
  }

  override protected def getPartitions: Array[Partition] = {
    // One-2-One partition
    val numPartitions = firstParent.getNumPartitions
    (0 until numPartitions).map(i =>
      new SearchPartition[T](i,
        options.getIndexationOptions.getRootIndexDirectory,
        firstParent.partitions(i))).toArray
  }

  override protected def getPreferredLocations(split: Partition): Seq[String] = {
    // Try to balance partitions accros executors
    val allIds = context.getExecutorIds()
    val ids = allIds.sliding(getNumPartitions)
    if (split.index < ids.size) {
      ids.toSeq(split.index)
    }
    super.getPreferredLocations(split)
  }
}

object SearchRDD {
  def apply[T: ClassTag](rdd: RDD[T],
                         options: SearchRDDOptions[T]
                         = SearchRDDOptions.defaultOptions().asInstanceOf[SearchRDDOptions[T]]): SearchRDD[T]
  = new SearchRDD(rdd, options)
}
