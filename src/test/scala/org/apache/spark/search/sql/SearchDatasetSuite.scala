package org.apache.spark.search.sql

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.search.TestData
import org.apache.spark.search.TestData.Company
import org.apache.spark.search.rdd.SearchRecord
import org.apache.spark.sql.Encoders
import org.scalatest.funsuite.AnyFunSuite

class SearchDatasetSuite extends AnyFunSuite with LocalSparkSession {


  test("A dataset can be full text queryable as list") {
    val spark = _spark

    val companies = TestData.companiesDS(spark).repartition(4).cache

    val l3Comm = companies.searchList("name:\"*3*communications\"~0.8")
    assertResult(Array(new SearchRecord(94, 3, 3.7266731f, 0, Company("l-3 communications",
      "l3design.eu", "1997.0", "management consulting", "10001+",
      "new york, new york, united states", "united states",
      "linkedin.com/company/l3", "12268", 30840))))(l3Comm)
  }

  test("A dataset can be serialized properly after search") {
    Encoders.bean(classOf[SearchRecord[_]])

    val spark = _spark
    import spark.implicits._

    val companies = TestData.companiesDS(spark).repartition(4).cache

    val jsonFile = new File(System.getProperty("java.io.tmpdir"), "spark-search-test-companies-test-1")
    if (jsonFile.exists()) {
      FileUtils.deleteDirectory(jsonFile)
    }

    val sanofi = companies.search("name:sanoffi~8").cache
    sanofi.map(_.source.name).foreach(println(_))
    sanofi.map(_.score).foreach(println(_))
    sanofi.foreach(println(_))

    sanofi.write.json(jsonFile.getAbsolutePath)

    val companiesLoaded = spark.read.json(jsonFile.getAbsolutePath).as[SearchRecord[Company]]
    val cc = companiesLoaded.collect()

    assertResult(Array(SearchRecord(12, 3, 2.4564871788024902, 0,
      Company("sanofi", "sanofi.com", "", "pharmaceuticals", "10001+", "paris, île-de-france, france", "france", "linkedin.com/company/sanofi", "38533", 99940))))(companiesLoaded.collect())
  }

  test("A dataset can be searched and searched results processed as dataset") {
    val spark = _spark
    import spark.implicits._

    val companies = TestData.companiesDS(spark).repartition(4).cache

    assertResult(1000)(companies.count)

    assertResult(1)(companies.search("name:IBM").count)
    //assertResult("ibm.com")(companies.search("name:IBM").map(_.source.domain))

    //assertResult(Array("massgeneral.org"))(companies.search("name:\"masachusetts general hospital\"").map(_.source.domain).collect)
  }

}
