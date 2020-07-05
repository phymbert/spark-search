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

import org.apache.commons.io.FileUtils
import org.apache.spark.search.rdd.TestData.persons
import org.scalatest.flatspec.AnyFlatSpec
import ZipUtils._

class SearchIndexRDDSuite extends AnyFlatSpec with LocalSparkContext {

  it should "creates zip index and stream to the next rdd" in {
    val searchIndexedRDD = sc.parallelize(persons).searchRDD.searchIndexRDD
    searchIndexedRDD.count()
    val indexDirectoryByPartition = searchIndexedRDD._indexDirectoryByPartition
    indexDirectoryByPartition.foreach(t => {
      val indexDir = new File(t._2)
      FileUtils.deleteDirectory(indexDir)
      FileUtils.deleteQuietly(new File(indexDir.getParent, s"${indexDir.getName}.zip"))
    })

    searchIndexedRDD.mapPartitionsWithIndex((t, p) => {
      unzipPartition(indexDirectoryByPartition(t), p)
      Iterator()
    }, preservesPartitioning = true).count()
  }

}
