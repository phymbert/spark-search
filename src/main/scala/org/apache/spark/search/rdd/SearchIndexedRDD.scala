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

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.search._
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
private[search] class SearchIndexedRDD[T: ClassTag](rdd: RDD[T],
                                                    val options: SearchOptions[T])
  extends RDD[T](rdd.context, Seq(new OneToOneDependency(rdd))) {

  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    val searchRDDPartition = split.asInstanceOf[SearchPartitionIndex[T]]

    val elements = firstParent.iterator(searchRDDPartition.parent, context).asJava
      .asInstanceOf[java.util.Iterator[T]]
    searchRDDPartition.index(elements, options.getIndexationOptions)

    Iterator.empty
  }

  override protected def getPartitions: Array[Partition] = {
    // One-2-One partition
    firstParent.partitions.map(p =>
      new SearchPartitionIndex[T](p.index,
        s"${
          options.getIndexationOptions.getRootIndexDirectory
        }-rdd${id}", p)).toArray
  }

  override protected[rdd] def getPreferredLocations(split: Partition): Seq[String] = {
    // Try to balance partitions across executors
    val allIds = context.getExecutorIds()
    if (allIds.nonEmpty) {
      val ids = allIds.sliding(getNumPartitions).toList
      ids(split.index % ids.length)
    } else {
      super.getPreferredLocations(split)
    }
  }

  lazy val _indexDirectoryByPartition: Map[Int, String] =
    partitions.map(_.asInstanceOf[SearchPartition[T]]).map(t => (t.index, t.searchIndexPartition.indexDir)).toMap

  override def unpersist(blocking: Boolean): SearchIndexedRDD.this.type = {
    // FIXME support blocking
    val indexDirectoryByPartition = _indexDirectoryByPartition

    sparkContext.runJob(this, (context: TaskContext, _: Iterator[T]) => {
      FileUtils.deleteDirectory(new File(indexDirectoryByPartition(context.partitionId())))
    })
    super.unpersist(blocking)
  }
}

