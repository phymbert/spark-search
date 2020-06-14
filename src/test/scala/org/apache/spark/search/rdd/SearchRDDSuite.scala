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

import org.apache.lucene.analysis.shingle.ShingleAnalyzerWrapper
import org.apache.lucene.search.BooleanClause.Occur
import org.apache.lucene.util.QueryBuilder
import org.apache.spark.api.java.StorageLevels
import org.apache.spark.search.TestData._
import org.apache.spark.search.{SearchException, _}
import org.apache.spark.sql.SparkSession
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
      Person("Bob", "Marley", 37))))(sc.parallelize(persons).searchList("firstName:bob", 10))
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
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("Spark Search Test")
      .getOrCreate()
    import spark.implicits._
    val companies = TestData.companiesDS(spark).repartition(4).as[Company].cache.rdd

    val searchRDD = companies.searchJoin[Company](companies, (_: Company) => "name:ibm", 1)
      .filter(_.hits.count(_.source.name.equals("ibm")) == 1)
    assertResult(1000)(searchRDD.count)

    spark.stop
  }

  test("self search join with self query value should returns all document with self match") {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("Spark Search Test")
      .getOrCreate()
    import spark.implicits._
    val companies = TestData.companiesDS(spark).repartition(4).as[Company].cache.rdd

    val searchRDD = companies.searchQueryJoin(companies,
      queryBuilder[Company]((c: Company, lqb: QueryBuilder) => lqb.createBooleanQuery("name", c.name, Occur.MUST)),
      topK = 1,
      minScore = 0,
      opts = SearchOptions.builder().analyzer(classOf[ShingleAnalyzerWrapper]).build())
      .filter(m => m.hits.count(h => h.source.name.equals(m.doc.name)) == 1)
      .cache
    searchRDD.map(m => (m.doc.name, m.hits.map(h => (h.score, h.source.name)).mkString(", "))).foreach(println)
    assertResult(1000)(searchRDD.count)

    spark.stop
  }

  test("Should convert to product") {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("Spark Search Test")
      .getOrCreate()
    import spark.implicits._
    val companies = TestData.companiesDS(spark).repartition(4).as[Company].cache.rdd
    val companies2 = TestData.companiesEdgarDS(spark).repartition(4).as[SecEdgarCompanyInfo].cache.rdd

    val matchRDD = companies.searchQueryJoin(companies2,
      queryBuilder[SecEdgarCompanyInfo]((c: SecEdgarCompanyInfo, lqb: QueryBuilder) => lqb.createBooleanQuery("name", c.companyName, Occur.MUST)),
      topK = 1,
      minScore = 0,
      opts = SearchOptions.builder().analyzer(classOf[ShingleAnalyzerWrapper]).build())
      .cache
    matchRDD.map(m => (m.doc.companyName, m.hits.map(h => (h.score, h.source.name)).mkString(", "))).foreach(println)
    assertResult(10003)(matchRDD.count)

    spark.stop
  }
}