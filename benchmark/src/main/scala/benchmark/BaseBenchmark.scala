package benchmark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

abstract class BaseBenchmark(appName: String) extends Serializable {
  val spark: SparkSession = SparkSession.builder().appName(appName).getOrCreate()

  protected def run(): Unit = {
    import spark.implicits._

    // https://www.kaggle.com/peopledatalabssf/free-7-million-company-dataset
    val companies = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("hdfs:///companies_sorted.csv")
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
    val secEdgarCompanies = spark.read.option("header", "true")
      .option("inferSchema", "true")
      .csv("hdfs:///sec__edgar_company_info.csv")
      .withColumnRenamed("Line Number", "lineNumber")
      .withColumnRenamed("Company Name", "companyName")
      .withColumnRenamed("Company CIK Key", "companyCIKKey")
      .as[SecEdgarCompanyInfo]
      .repartition(2, $"lineNumber")
      .cache
      .rdd
    secEdgarCompanies.count

    // Count matches
    var startTime = System.currentTimeMillis()
    val matches = countNameMatches(companies, "IBM").cache
    var count = matches.count
    var endTime = System.currentTimeMillis()
    println(s"Count ${count} matches in ${(endTime.toFloat - startTime.toFloat)}ms")
    matches.take(100).foreach(println(_))
    matches.unpersist()

    // Join matches
    startTime = System.currentTimeMillis()
    val joinedMatches = joinMatch(companies, secEdgarCompanies).cache
    count = joinedMatches.count
    endTime = System.currentTimeMillis()
    println(s"Joined ${count} matches in ${(endTime.toFloat - startTime.toFloat) / 1000f}s")
    joinedMatches.take(100).foreach(println(_))
    joinedMatches.unpersist()

    spark.stop()
  }

  def countNameMatches(companies: RDD[Company], name: String): RDD[(Double, String)]

  def joinMatch(companies: RDD[Company], secEdgarCompany: RDD[SecEdgarCompanyInfo]): RDD[(String, Double, String)]
}
