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

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.search.{IndexationOptions, SearchOptions}
import org.apache.spark.search.rdd.TestData._
import org.apache.spark.search.rdd.ZipUtils._
import org.scalatest.flatspec.AnyFlatSpec

class SearchRDDReloadedSuite extends AnyFlatSpec with LocalSparkContext {

  it should "Reload zip indices and stream to the next rdd" in {
    val path = "target/test-save-and-reload"
    FileUtils.deleteDirectory(new File(path))

    val searchRDD = sc.parallelize(persons).repartition(3).searchRDD()
    assertResult(3)(searchRDD.count())

    searchRDD.save(path)

    val restoredSearchRDD = SearchRDD.load[Person](sc, path).asInstanceOf[SearchRDDImpl[Person]]
    assertResult(3)(restoredSearchRDD.count())

    assertResult(Array("Bartoll", "Marley", "Michael"))(restoredSearchRDD
      .map(_.lastName)
      .collect().sorted)

    val reloadedRDD: SearchRDDReloaded[Person] = restoredSearchRDD.indexerRDD.asInstanceOf[SearchRDDReloaded[Person]]
    val indexDirectoryByPartition = reloadedRDD._indexDirectoryByPartition
    restoredSearchRDD.indexerRDD.mapPartitionsWithIndex((t, p) => {
      unzipPartition(indexDirectoryByPartition(t), p)
      Iterator()
    }, preservesPartitioning = true).count()
  }


  it should "Reload zip indices and stream to the next rdd without copyto local" in {
    val path = "target/test-save-and-reload"
    FileUtils.deleteDirectory(new File(path))

    val searchRDD = sc.parallelize(persons).repartition(3).searchRDD()
    assertResult(3)(searchRDD.count())

    searchRDD.save(path)

    val restoredSearchRDD = SearchRDD.load[Person](sc,
      path,
      SearchOptions
        .builder[Person]()
        .index((r: IndexationOptions.Builder[Person]) =>
          r.reloadIndexWithHdfsCopyToLocal(false))
        .build()
    )
    assertResult(3)(restoredSearchRDD.count())

    assertResult(Array("Bartoll", "Marley", "Michael"))(restoredSearchRDD
      .map(_.lastName)
      .collect().sorted)
  }

}
