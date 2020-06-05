package org.apache.spark.search.sql

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.lucene.analysis.shingle.ShingleAnalyzerWrapper
import org.apache.spark.search.TestData.{Company, SecEdgarCompanyInfo}
import org.apache.spark.search._
import org.apache.spark.sql.Encoders
import org.scalatest.funsuite.AnyFunSuite

class SearchDatasetSuite extends AnyFunSuite with LocalSparkSession {


  test("A dataset can be full text searched") {
    val spark = _spark

    val companies = TestData.companiesDS(spark).repartition(4).cache

    val l3Comm = companies.searchList("name:\"*3*communications\"~0.8")
    assertResult(Array(SearchRecord(94, 3, 3.7266731f, 0, Company("l-3 communications",
      "l3design.eu", "1997.0", "management consulting", "10001+",
      "new york, new york, united states", "united states",
      "linkedin.com/company/l3", "12268", 30840))))(l3Comm)
  }

  test("A dataset can be serialized properly after search") {
    Encoders.bean(classOf[SearchRecord[_]])

    val spark = _spark

    val companies = TestData.companiesDS(spark).repartition(4).cache

    val jsonFile = new File(System.getProperty("java.io.tmpdir"), "spark-search-test-companies-test-1")
    if (jsonFile.exists()) {
      FileUtils.deleteDirectory(jsonFile)
    }

    val sanofi = companies.search("name:sanoffi~8")

    sanofi.write.json(jsonFile.getAbsolutePath)

    val companiesLoaded = spark.read.json(jsonFile.getAbsolutePath).as[SearchRecord[Company]]

    assertResult(Array(SearchRecord(12, 3, 2.4564871788024902, 0,
      Company("sanofi", "sanofi.com", "", "pharmaceuticals", "10001+", "paris, île-de-france, france", "france", "linkedin.com/company/sanofi", "38533", 99940))))(companiesLoaded.collect())
  }

  test("A dataset can be searched and searched results processed as dataset") {
    val spark = _spark
    import spark.implicits._

    val companies = TestData.companiesDS(spark).repartition(4).cache

    assertResult(1000)(companies.count)

    assertResult(1)(companies.search("name:IBM").count)
    assertResult("ibm.com")(companies.search("name:IBM").map(_.source.domain).first)

    assertResult(Array("massgeneral.org", "mit.edu"))(companies.search("name:masachusetts~0.4").map(_.source.domain).collect)
  }

  test("A dataset can be joined with another one by search queries") {
    val spark = _spark

    val companies = TestData.companiesDS(spark).repartition(4).cache
    val secCompanies = TestData.companiesEdgarDS(spark).repartition(4).cache

    var matchedCompanies = companies.searchJoin(secCompanies,
      (c: SecEdgarCompanyInfo) => s"name:${"\"" + c.companyName.slice(0, 32).replaceAllLiterally("\"", "\\\"") + "\""}",
      topK = 1,
      opts = SearchOptions
        .builder[Company]
        .analyzer(classOf[ShingleAnalyzerWrapper]).build())
      .filter(_.hits.nonEmpty)

    val jsonFile = new File(System.getProperty("java.io.tmpdir"), "spark-search-test-companies-test-2")
    if (jsonFile.exists()) {
      FileUtils.deleteDirectory(jsonFile)
    }

    matchedCompanies.write.json(jsonFile.getAbsolutePath)

    matchedCompanies = _spark.read.json(jsonFile.getAbsolutePath).as[Match[SecEdgarCompanyInfo, Company]]

    matchedCompanies.foreach(println(_))
    //assertResult(3)(matchedCompanies.count)
  }

  test("A dataset can drop duplicates") {
    val spark = _spark
    import spark.implicits._

    val secCompanies = TestData.companiesEdgarDS(spark).repartition(4).cache

    assertResult(10003)(secCompanies.count)
    val deduplicated = secCompanies.searchDropDuplicates(defaultQueryBuilder[SecEdgarCompanyInfo](), 3, 1)
    deduplicated.foreach(println(_))
    assertResult(2513)(deduplicated.count)
  }
}
