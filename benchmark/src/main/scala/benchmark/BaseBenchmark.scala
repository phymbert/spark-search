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
import org.apache.spark.sql.{SaveMode, SparkSession}

abstract class BaseBenchmark(appName: String) extends Serializable {
  val spark: SparkSession = SparkSession.builder().appName(appName).getOrCreate()

  protected def run(): Unit = {
    import spark.implicits._

    // Convert CSV to parquet

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
      .write
      .mode(SaveMode.Ignore)
      .parquet("hdfs:///companies_sorted.parquet")

    // https://www.kaggle.com/dattapiy/sec-edgar-companies-list
    spark.read.option("header", "true")
      .option("inferSchema", "true")
      .csv("hdfs:///sec__edgar_company_info.csv")
      .withColumnRenamed("Line Number", "lineNumber")
      .withColumnRenamed("Company Name", "companyName")
      .withColumnRenamed("Company CIK Key", "companyCIKKey")
      .write
      .mode(SaveMode.Ignore)
      .parquet("hdfs:///sec__edgar_company_info.parquet")

    def loadCompanies = {
      spark.read
        .load("hdfs:///companies_sorted.parquet")
        .as[Company]
        .rdd
    }

    def loadSecEdgarCompanies = {
      spark.read.load("hdfs:///sec__edgar_company_info.parquet")
        .as[SecEdgarCompanyInfo]
        .rdd
    }

    // Join matches
    (0 until 5).foreach(i => {
      val (count, joinedMatches) = spark.time({
        val jm = joinMatch(loadCompanies, loadSecEdgarCompanies).cache
        (jm.count, jm)
      })
      println(s"for joined ${count} matches")
      if (i == 0) {
        joinedMatches.take(100).foreach(println(_))
      }
      joinedMatches.unpersist()
    })

    // Count matches
    (0 until 5).foreach(i => {
      val (count, matches) = spark.time({
        val m = countNameMatches(loadCompanies, "IBM").cache
        (m.count, m)
      })
      println(s"for count ${count} matches")
      if (i == 0) {
        matches.take(100).foreach(println(_))
      }
      matches.unpersist()
    })

    spark.stop()
  }

  def countNameMatches(companies: RDD[Company], name: String): RDD[(Double, String)]

  def joinMatch(companies: RDD[Company], secEdgarCompany: RDD[SecEdgarCompanyInfo]): RDD[(String, Double, String)]
}
