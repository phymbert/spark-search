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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path, PathFilter}
import org.apache.spark.search.SearchOptions
import org.apache.spark.{Partition, SparkContext, TaskContext}

import scala.reflect.ClassTag

/**
 * Reloaded index from hdfs.
 *
 * @author Pierrick HYMBERT
 */
private[search] class SearchIndexReloadedRDD[S: ClassTag](sc: SparkContext,
                                                          path: String,
                                                          override val options: SearchOptions[S])
  extends SearchRDDIndexer[S](sc, options, Nil) {

  override protected def getPartitions: Array[Partition] = {
    val hadoopConf = new Configuration()
    val hdfs = FileSystem.get(hadoopConf)
    val partitionsZipped = hdfs.listStatus(new Path(path), new PathFilter {
      override def accept(path: Path): Boolean = path.getName.endsWith(".zip")
    }).zipWithIndex

    partitionsZipped.map(p => new SearchIndexReloadedPartition(p._2, rootDir, p._1.getPath.toUri.toString,
      getPreferredLocation(context, p._2, partitionsZipped.length, Seq())))
  }

  override def compute(split: Partition, context: TaskContext): Iterator[Array[Byte]] = {
    val part = split.asInstanceOf[SearchIndexReloadedPartition]
    val hadoopConf = new Configuration()
    val hdfs = FileSystem.get(hadoopConf)
    ZipUtils.unzipPartition(part.indexDir, hdfs.open(new Path(part.zipPath)))
    streamPartitionIndexZip(context, part.asInstanceOf[SearchPartitionIndex[S]])
  }
}

class SearchIndexReloadedPartition(val idx: Int,
                                   val rootDir: String,
                                   val zipPath: String,
                                   val preferredLocations2: Array[String])
  extends SearchPartitionIndex(idx, rootDir, preferredLocations2, null) {
}