package benchmark.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.search.TestData.{Company, SecEdgarCompanyInfo}
import org.apache.spark.sql.SparkSession

abstract class BaseBenchmark extends Serializable {
  def run(spark: SparkSession, f: (SparkSession, RDD[Company], RDD[SecEdgarCompanyInfo]) => Unit): Unit = {

    import spark.implicits._

    // https://www.kaggle.com/peopledatalabssf/free-7-million-company-dataset
    val companies = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("/user/zeppelin/companies.csv")
      .withColumnRenamed("year founded", "yearFounded")
      .withColumnRenamed("size range", "sizeRange")
      .withColumnRenamed("linkedin url", "linkedinUrl")
      .withColumnRenamed("current employee estimate", "currentEmployeeEstimate")
      .withColumnRenamed("total employee estimate", "totalEmployeeEstimate")
      .withColumnRenamed("_c0", "id")
      .na.fill("", Seq("domain", "yearFounded", "industry", "sizeRange", "locality", "country", "linkedinUrl", "currentEmployeeEstimate", "totalEmployeeEstimate"))
      .as[Company]
      .repartition(7, $"id")
      .cache
      .rdd
    companies.count

    // https://www.kaggle.com/dattapiy/sec-edgar-companies-list
    val secEdgarCompany = spark.read.option("header", "true")
      .option("inferSchema", "true")
      .csv("/user/zeppelin/sec__edgar_company_info.csv")
      .withColumnRenamed("Line Number", "lineNumber")
      .withColumnRenamed("Company Name", "companyName")
      .withColumnRenamed("Company CIK Key", "companyCIKKey")
      .as[SecEdgarCompanyInfo]
      .repartition(2, $"lineNumber")
      .cache
      .rdd
    secEdgarCompany.count

    val startTime = System.currentTimeMillis()
    f(spark, companies, secEdgarCompany)
    val endTime = System.currentTimeMillis()
    println(s"Time: ${(endTime.toFloat - startTime) / 1000}s")
  }
}
