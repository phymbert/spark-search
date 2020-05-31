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

import org.apache.spark.SparkContext
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite
import TestData._

class SearchRDDSuite extends AnyFunSuite with BeforeAndAfter with LocalSparkContext {

  test("count all indexed documents") {
    sc = new SparkContext("local", "test")

    assertResult(3)(sc.parallelize(persons).searchRDD.count)
  }

  test("count matched indexed documents") {
    sc = new SparkContext("local", "test")

    assertResult(1)(sc.parallelize(persons)
        .count("firstName:bob"))
  }

  test("search hits matching query") {
    sc = new SparkContext("local", "test")

    assertResult(List(new SearchRecord[Person](1, 0,0.44583148f, 0,
            Person("Bob", "Marley", 37))))(sc.parallelize(persons).search("firstName:bob",10))
  }
}