/**
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

import java.io.File
import java.nio.file.Files
import java.util.function.Consumer
import java.util.zip.{ZipEntry, ZipOutputStream}

import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.search._
import org.apache.spark._

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
  extends RDD[T](sc, deps) {

  def this(rdd: RDD[T], options: SearchOptions[T]) {
    this(rdd.context, options, Seq(new OneToOneDependency(rdd)))
  }


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
      val zip = new ZipOutputStream(fos)
      Files.list(localIndexDirPath).forEach {
        new Consumer[java.nio.file.Path] {
          override def accept(file: java.nio.file.Path): Unit = {
            zip.putNextEntry(new ZipEntry(file.toFile.getName))
            Files.copy(file, zip)
            zip.closeEntry()
          }
        }
      }
      zip.close()
      fos.close()
      logInfo(s"Partition ${localIndexDirPath} saved to ${targetPath}")
      Iterator()
    }).collect
  }

  override def unpersist(blocking: Boolean): SearchIndexedRDD.this.type = {
    // TODO support non blocking
    val indexDirectoryByPartition = _indexDirectoryByPartition
    sparkContext.runJob(this, (context: TaskContext, _: Iterator[T]) => {
      FileUtils.deleteDirectory(new File(indexDirectoryByPartition(context.partitionId())))
    })
    super.unpersist(blocking)
  }
}

