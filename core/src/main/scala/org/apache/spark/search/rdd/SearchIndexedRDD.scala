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

import java.io._
import java.util

import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.search._
import org.apache.spark.search.rdd.ZipUtils._

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

/**
 * A search RDD indexes parent RDD partitions to lucene indexes.
 * It builds for all parent RDD partitions a one-2-one volatile Lucene index
 * available during the lifecycle of the spark session across executors local directories and RAM.
 *
 * @author Pierrick HYMBERT
 */
private[search] class SearchIndexedRDD[T: ClassTag](sc: SparkContext,
                                                    val options: SearchOptions[T],
                                                    val deps: Seq[Dependency[_]])
  extends RDD[Array[Byte]](sc, deps) {

  def this(rdd: RDD[T], options: SearchOptions[T]) {
    this(rdd.context, options, Seq(new OneToOneDependency(rdd)))
  }

  override def compute(split: Partition, context: TaskContext): Iterator[Array[Byte]] = {
    val searchRDDPartition = split.asInstanceOf[SearchPartitionIndex[T]]

    val elements = firstParent.iterator(searchRDDPartition.parent, context).asJava
      .asInstanceOf[util.Iterator[T]]
    searchRDDPartition.index(elements, options.getIndexationOptions)

    streamPartitionIndexZip(context, searchRDDPartition)
  }

  protected def streamPartitionIndexZip(context: TaskContext, searchRDDPartition: SearchPartitionIndex[T]): Iterator[Array[Byte]] = {
    val localIndexDirPath = new File(searchRDDPartition.indexDir)
    val targetPath = new File(localIndexDirPath.getParent, s"${localIndexDirPath.getName}.zip")
    zipPartition(localIndexDirPath.toPath, new FileOutputStream(targetPath))

    new InterruptibleIterator[Array[Byte]](context, new FileInputStreamIterator(targetPath))
  }

  override protected def getPartitions: Array[Partition] = {
    // One-2-One partition
    firstParent.partitions.map(p =>
      new SearchPartitionIndex[T](p.index,
        rootDir, p)).toArray
  }

  def rootDir: String =
    s"${options.getIndexationOptions.getRootIndexDirectory}-rdd${id}"

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
    partitions.map(_.asInstanceOf[SearchPartitionIndex[T]]).map(t => (t.index, t.indexDir)).toMap

  def save(path: String): Unit = {
    val indexDirectoryByPartition = _indexDirectoryByPartition
    mapPartitionsWithIndex((index, _) => {
      val hadoopConf = new Configuration()
      val hdfs = FileSystem.get(hadoopConf)
      val localIndexDirPath = new File(indexDirectoryByPartition(index)).toPath
      val targetPath = new Path(path, s"${localIndexDirPath.getFileName}.zip")
      logInfo(s"Saving partition ${localIndexDirPath} to ${targetPath}")
      val fos = hdfs.create(targetPath)
      zipPartition(localIndexDirPath, fos)
      logInfo(s"Partition ${localIndexDirPath} saved to ${targetPath}")
      Iterator()
    }).collect
  }



  override def unpersist(blocking: Boolean): SearchIndexedRDD.this.type = {
    // TODO support non blocking
    val indexDirectoryByPartition = _indexDirectoryByPartition
    sparkContext.runJob(this, (context: TaskContext, _: Iterator[Array[Byte]]) => {
      val indexDir = new File(indexDirectoryByPartition(context.partitionId()))
      FileUtils.deleteDirectory(indexDir)
      FileUtils.deleteQuietly(new File(indexDir.getParent, s"${indexDir.getName}.zip"))
    })
    super.unpersist(blocking)
  }
}
private[rdd] object SearchIndexedRDD {

}