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

package benchmark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

abstract class BaseBenchmark(appName: String) extends Serializable {
  val spark: SparkSession = SparkSession.builder().appName(appName).getOrCreate()

  protected def run(): Unit = {
    import spark.implicits._

    def loadCompanies = {
      // https://www.kaggle.com/peopledatalabssf/free-7-million-company-dataset
      spark.read
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
        .rdd
    }

    def loadSecEdgarCompanies = {
      // https://www.kaggle.com/dattapiy/sec-edgar-companies-list
      spark.read.option("header", "true")
        .option("inferSchema", "true")
        .csv("hdfs:///sec__edgar_company_info.csv")
        .withColumnRenamed("Line Number", "lineNumber")
        .withColumnRenamed("Company Name", "companyName")
        .withColumnRenamed("Company CIK Key", "companyCIKKey")
        .as[SecEdgarCompanyInfo]
        .repartition(2, $"lineNumber")
        .rdd
    }

    // Count matches
    var startTime = System.currentTimeMillis()
    val matches = countNameMatches(loadCompanies, "IBM").cache
    var count = matches.count
    var endTime = System.currentTimeMillis()
    println(s"Count ${count} matches in ${(endTime.toFloat - startTime.toFloat) / 1000f}s")
    matches.take(100).foreach(println(_))
    matches.unpersist()

    // Join matches
    startTime = System.currentTimeMillis()
    val joinedMatches = joinMatch(loadCompanies, loadSecEdgarCompanies).cache
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
