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
import org.apache.lucene.search.BooleanClause.Occur
import org.apache.lucene.util.QueryBuilder
import org.apache.spark.api.java.StorageLevels
import org.apache.spark.search.rdd.TestData._
import org.apache.spark.search.{SearchException, _}
import org.scalatest.funsuite.AnyFunSuite

import scala.language.implicitConversions

class SearchRDDSuite extends AnyFunSuite with LocalSparkContext {

  test("count all indexed documents") {
    assertResult(3)(sc.parallelize(persons).count)
  }

  test("count matched indexed documents") {
    assertResult(1)(sc.parallelize(persons)
      .count("firstName:bob"))
  }

  test("search list hits matching query") {
    assertResult(Array(new SearchRecord[Person](0, 1, 0.3150668740272522f, 0,
      Person("Bob", "Marley", 37))))(sc.parallelize(persons)
      .searchList("firstName:bob", 10))
  }

  test("search RDD hits matching query") {
    assertResult(Array(new SearchRecord[Person](0, 1, 0.3150668740272522f, 0,
      Person("Bob", "Marley", 37))))(sc.parallelize(persons).search("firstName:bob", 10).take(10))
  }

  test("Matching RDD") {
    val persons2 = Seq(
      Person("George", "Michal", 0),
      Person("Georgee", "Michall", 0),
      Person("Bobb", "Marley", 0),
      Person("Bob", "Marlley", 0),
      Person("Agnes", "Bartol", 0),
      Person("Agnec", "Barttol", 0))

    val searchRDD = sc.parallelize(persons2).repartition(2).searchRDD
    val matchingRDD = sc.parallelize(persons)

    val matches = searchRDD.searchJoin(matchingRDD, (p: Person) => s"firstName:${p.firstName}~0.5 AND lastName:${p.lastName}~0.5", 2).collect
    assertResult(3)(matches.length)
    assertResult(3)(matches.map(m => m.hits.length).count(_ == 2))
  }

  test("Persisting RDD to local dirs is forbidden") {
    val searchRDD = sc.parallelize(Seq(Person("Georges", "Brassens", 99))).searchRDD
    assertThrows[SearchException] {
      searchRDD.persist(StorageLevels.MEMORY_AND_DISK)
    }
    searchRDD.cache
  }

  test("self search join with one query value should returns all document with one match") {
    val rdd = sc.parallelize(persons)
    val searchRDD = rdd
      .searchJoin(rdd, (_: Person) => "lastName:Marley", 1)
      .filter(_.hits.count(_.source.lastName.equals("Marley")) == 1)
    assertResult(3)(searchRDD.count)
  }

  test("self search join with self query value should returns all document with self match") {
    val rdd = sc.parallelize(persons)
    val searchRDD = rdd.searchQueryJoin(rdd,
      queryBuilder((c: Person, lqb: QueryBuilder) => lqb.createBooleanQuery("firstName", c.firstName, Occur.MUST)),
      topK = 1,
      minScore = 0)
      .filter(m => m.hits.count(h => h.source.firstName.equals(m.doc.firstName)) == 1)
      .cache
    assertResult(3)(searchRDD.count)
  }

  test("search join should work") {
    val opts = SearchOptions.builder[Person]().analyzer(classOf[TestPersonAnalyzer]).build()
    val searchRDD = sc.parallelize(persons2).repartition(1)
      .searchQueryJoin(sc.parallelize(persons),
        queryBuilder((c: Person, lqb: QueryBuilder) => lqb.createBooleanQuery("firstName", c.firstName), opts),
        2,
        0,
        opts)
      .filter(_.hits.length == 2)
    assertResult(3)(searchRDD.count)
  }

  test("Distinct with no minimum score test RDD") {
    val searchRDD = sc.parallelize(persons2).repartition(1).searchRDD
    val deduplicated = searchRDD.distinct().collect
    assertResult(1)(deduplicated.length)
  }

  test("Drop duplicate with min score test RDD") {
    val searchRDD = sc.parallelize(persons2).repartition(1)
      .searchRDD(opts = SearchOptions.builder().analyzer(classOf[TestPersonAnalyzer]).build())

    val deduplicated = searchRDD.searchDropDuplicates(minScore = 8).collect
    assertResult(3)(deduplicated.length)
  }

  test("Save and restore index from/to hdfs") {
    FileUtils.deleteDirectory(new File("target/test-save"))

    val searchRDD = sc.parallelize(persons).searchRDD
    assertResult(3)(searchRDD.count())

    searchRDD.save("target/test-save")

    val restoredSearchRDD = SearchRDD.load[Person](sc, "target/test-save")
    assertResult(3)(restoredSearchRDD.count)
  }
}