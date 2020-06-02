package org.apache.spark.search.sql

import org.apache.spark.search.TestData
import org.scalatest.funsuite.AnyFunSuite

class SearchDatasetSuite extends AnyFunSuite with LocalSparkSession {

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
