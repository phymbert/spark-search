package org.apache.spark.search.sql

import org.scalatest.flatspec.AnyFlatSpec

class SearchDatasetSuite extends AnyFlatSpec with LocalSparkSession {

  ignore should "be searchable" in {
    val spark = _spark
    import spark.sqlContext.implicits._

    val companies1 = TestData.companies1DS(spark).repartition(4).cache

    val appleCompany = companies1
      .where($"name".matches("apple") && score() > 1d)

    appleCompany.show

    assertResult(1) {
      appleCompany.count
    }
  }
}
