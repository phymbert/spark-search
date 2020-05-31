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

import org.apache.spark.rdd.RDD
import org.apache.spark.{OneToOneDependency, Partition, TaskContext}

import scala.collection.JavaConverters._
import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

/**
 * A search RDD indexes parent RDD partitions to lucene indexes.
 * Offers search features.
 *
 * Limitation: At the moment it cannot be neither persisted or restored or moved.
 *
 * @author Pierrick HYMBERT
 */
private[search] class SearchRDD[T: ClassTag](rdd: RDD[T],
                                             val options: SearchRDDOptions[T])
  extends RDD[T](rdd.context, Seq(new OneToOneDependency(rdd))) {

  /**
   * Class of the first element indexed, will be used after
   * in [[DocumentConverter]] to recompose the source.
   */
  private var _clazz: Class[T] = _

  override def count: Long = runSearchJob[Long, Long](searchPartitionReader => searchPartitionReader.count(), _.sum)

  /**
   * Count how many documents match the given query.
   */
  def count(query: String): Long = runSearchJob[Long, Long](searchPartitionReader => searchPartitionReader.count(query), _.sum)

  /**
   * Finds the top topK hits for query.
   */
  def search(query: String, topK: Int): List[SearchRecord[T]] =
    runSearchJob[List[SearchRecord[T]], List[SearchRecord[T]]](
      searchPartitionReader => searchPartitionReader.search(query, topK).asScala.toList,
      _.reduce(_ ++ _).sortBy(_.getScore)(Ordering[Float].reverse).take(topK))

  protected def runSearchJob[R: ClassTag, A: ClassTag](searchByPartition: (SearchPartitionReader[T]) => R,
                                                       reducer: (Iterator[R]) => A): A = {

    val indexDirectoryByPartition = partitions.map(_.asInstanceOf[SearchPartition[T]]).zipWithIndex
      .map(t => (t._2, t._1.indexDir)).toMap

    val ret = sparkContext.runJob(this, (context: TaskContext, _: Iterator[T]) => {
      val index = context.partitionId()
      tryAndClose(new SearchPartitionReader[T](index, indexDirectoryByPartition(index), _clazz, options.getReaderOptions)) {
        r => searchByPartition(r)
      }
    })
    reducer(ret.toIterator)
  }

  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    val searchRDDPartition = split.asInstanceOf[SearchPartition[T]]

    val elements = firstParent.iterator(searchRDDPartition.parent, context).asJava
      .asInstanceOf[java.util.Iterator[T]]
    _clazz = searchRDDPartition.index(elements, options.getIndexationOptions)

    Iterator.empty // No RDD expected after
  }

  override protected def getPartitions: Array[Partition] = {
    // One-2-One partition
    firstParent.partitions.map(p =>
      new SearchPartition[T](p.index,
        options.getIndexationOptions.getRootIndexDirectory, p)).toArray
  }

  override protected def getPreferredLocations(split: Partition): Seq[String] = {
    // Try to balance partitions across executors
    val allIds = context.getExecutorIds()
    val ids = allIds.sliding(getNumPartitions)
    if (split.index < ids.size) {
      ids.toSeq(split.index)
    }
    super.getPreferredLocations(split)
  }

  def tryAndClose[A <: AutoCloseable, B](resource: A)(block: A => B): B = {
    Try(block(resource)) match {
      case Success(result) =>
        resource.close()
        result
      case Failure(e) =>
        resource.close()
        throw e
    }
  }
}

