package org.apache.spark.search.sql

import org.apache.spark.search.TestData
import org.scalatest.funsuite.AnyFunSuite

class SearchDataframeSuite extends AnyFunSuite with LocalSparkSession {

  test("A search dataframe can be searchable") {
    val spark = _spark
    val companies = TestData.companiesDS(spark).repartition(4).cache.toDF

    val matchedCompanies = companies.search("name:\"Air France\"")

    matchedCompanies.foreach(println(_))
    assertResult(1)(matchedCompanies.count)
  }

  test("A search dataframe can be searched join") {
    val spark = _spark
    val companies = TestData.companiesDS(spark).repartition(4).cache.toDF

    val matchedCompanies = companies.search("name:\"Air France\"")

    matchedCompanies.foreach(println(_))
    assertResult(1)(matchedCompanies.count)
  }
}
