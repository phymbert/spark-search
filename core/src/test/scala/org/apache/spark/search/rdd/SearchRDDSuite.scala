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
import org.apache.lucene.analysis.en.EnglishAnalyzer
import org.apache.lucene.search.BooleanClause.Occur
import org.apache.lucene.util.QueryBuilder
import org.apache.spark.RangePartitioner
import org.apache.spark.api.java.StorageLevels
import org.apache.spark.rdd.RDD
import org.apache.spark.search.rdd.TestData._
import org.apache.spark.search.{SearchException, _}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers.{convertToAnyMustWrapper, sorted}

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
      Person("Bob", "Marley", 37))))(
      sc.parallelize(persons)
        .search("firstName:bob", 10).take(10)
    )
  }

  test("search RDD hits matching fuzzy query should return topK per partition") {
    val personsKeyedBy = sc.parallelize(personsDuplicated)
      .union(sc.parallelize(personsDuplicated))
      .zipWithIndex()
      .keyBy(_._2)

    val personsRDD = personsKeyedBy.partitionBy(new RangePartitioner(2, personsKeyedBy))
      .map(_._2._1)

    personsRDD.getNumPartitions mustBe 2

    val matches = personsRDD
      // Bob Marley will have a better score than Agnès Bartoll and she should not be hit
      .search("firstName:agnès~ OR firstName:bob~ lastName:marley~", topKByPartition = 2)

    // Assert we have only one lucene index per partition
    matches.map(_.partitionIndex).distinct().sortBy(identity).collect() mustBe Array(0, 1)

    // Assert we only have bob
    matches.map(_.source.firstName).map(_.toLowerCase.substring(0, 3)).collect().distinct mustBe Array("bob")

    // Assert we have all bobs matched
    matches.count() mustBe 4

    // Should be sorted by score score descending
    matches.map(_.score).collect().reverse mustBe sorted
  }

  test("search RDD query must use default field") {
    assertResult(Array(new SearchRecord[Person](0, 1, 0.3150668740272522f, 0,
      Person("Bob", "Marley", 37))))(sc.parallelize(persons).searchRDD(
      SearchOptions.builder[Person]()
        .read((r: ReaderOptions.Builder[Person]) => r.defaultFieldName("firstName"))
        .analyzer(classOf[EnglishAnalyzer])
        .build()).search("bob", 10).take(10))
  }

  test("Matching RDD") {
    val persons2 = Seq(
      Person("George", "Michal", 0),
      Person("Georgee", "Michall", 0),
      Person("Bobb", "Marley", 0),
      Person("Bob", "Marlley", 0),
      Person("Agnes", "Bartol", 0),
      Person("Agnec", "Barttol", 0))

    val searchRDD = sc.parallelize(persons2).repartition(2).searchRDD()
    val matchingRDD = sc.parallelize(persons)

    val personsKeyed: RDD[(Long, Person)] = matchingRDD.zipWithIndex().map(_.swap)
    val matches = searchRDD.matches(personsKeyed, (p: Person) => s"firstName:${p.firstName}~0.5 AND lastName:${p.lastName}~0.5", 2).collect
    assertResult(3)(matches.length)
    assertResult(3)(matches.map(m => m._2.length).count(_ == 2))
  }

  test("Persisting RDD to local dirs is forbidden") {
    val searchRDD = sc.parallelize(Seq(Person("Georges", "Brassens", 99)))
      .searchRDD().asInstanceOf[RDD[Person]]
    assertThrows[SearchException] {
      searchRDD.persist(StorageLevels.MEMORY_AND_DISK)
    }
    searchRDD.cache
  }

  test("self matches with one query value should returns all document with one match") {
    val rdd = sc.parallelize(persons)
    val searchRDD = rdd
      .matches(rdd.zipWithIndex().map(_.swap), (_: Person) => "lastName:Marley", 1)
      .filter(_._2.map(_._2).count(_.source.lastName.equals("Marley")) == 1)
    assertResult(3)(searchRDD.count)
  }

  test("self matches with self query value should returns all document with self match") {
    val rdd = sc.parallelize(persons)
    val searchRDD = rdd.matchesQuery(rdd.zipWithIndex().map(_.swap),
      queryBuilder((c: Person, lqb: QueryBuilder) => lqb.createBooleanQuery("firstName", c.firstName, Occur.MUST)),
      topK = 1)
      .map(_._2)
      .filter(m => m.count(h => h._2.source.firstName.equals(h._1.firstName)) == 1)
      .cache
    assertResult(3)(searchRDD.count)
  }

  test("search join should work") {
    val opts = SearchOptions.builder[(Long, Person)]().analyzer(classOf[TestPersonAnalyzer]).build()
    val searchRDD = sc.parallelize(personsDuplicated).repartition(1)
      .zipWithIndex().map(_.swap)
      .searchRDD(opts)
      .searchJoin(sc.parallelize(persons),
        (c: Person) => s"firstName:${c.firstName}",
        topK = 2)
    assertResult(12)(searchRDD.count) // FIXME add good unit test
  }

  test("search matches should work") {
    val opts = SearchOptions.builder[Person]().analyzer(classOf[TestPersonAnalyzer]).build()
    val searchRDD = sc.parallelize(personsDuplicated).repartition(1)
      .searchRDD(opts)
      .matchesQuery(sc.parallelize(persons).zipWithIndex().map(_.swap),
        queryBuilder((c: Person, lqb: QueryBuilder) => lqb.createBooleanQuery("firstName", c.firstName), opts),
        topK = 2)
      .filter(_._2.length == 2)
    assertResult(3)(searchRDD.count)
  }

  test("Distinct with no minimum score") {
    val searchRDD = sc.parallelize(personsDuplicated).zipWithIndex().map(_.swap)
      .repartition(1)
      .searchRDD()
    val deduplicated = searchRDD.distinct(1).collect
    assertResult(1)(deduplicated.length)
  }

  test("Drop duplicates with min score") {
    val searchRDD = sc.parallelize(personsDuplicated).repartition(1).zipWithIndex().map(_.swap)
      .searchRDD(opts = SearchOptions.builder().analyzer(classOf[TestPersonAnalyzer]).build())

    val deduplicated = searchRDD.searchDropDuplicates(minScore = 8).collect
    assertResult(3)(deduplicated.length)
  }

  test("Drop duplicate with query builder and min score") {
    val searchRDD = sc.parallelize(personsDuplicated).repartition(1).zipWithIndex().map(_.swap)

    val deduplicated = searchRDD.searchDropDuplicates(
      queryBuilder = queryStringBuilder(p => s"firstName:${p.firstName}~0.5 AND lastName:${p.lastName}~0.5"),
      minScore = 1
    ).collect
    assertResult(3)(deduplicated.length)
  }

  test("SearchRDD should be iterable as an RDD") {
    val searchRDD = sc.parallelize(persons).searchRDD()
    assertResult(Array("Agnès", "Bob", "Geoorge"))(searchRDD
      .filter(_.firstName.nonEmpty)
      .map(_.firstName)
      .collect().sorted)
  }

  test("Save and restore index from/to hdfs") {
    FileUtils.deleteDirectory(new File("target/test-save"))

    val searchRDD = sc.parallelize(persons).searchRDD()
    assertResult(3)(searchRDD.count())

    searchRDD.save("target/test-save")

    val restoredSearchRDD = SearchRDD.load[Person](sc, "target/test-save")
    assertResult(3)(restoredSearchRDD.count())

    assertResult(Array("Bartoll", "Marley", "Michael"))(restoredSearchRDD
      .map(_.lastName)
      .collect().sorted)

  }
}