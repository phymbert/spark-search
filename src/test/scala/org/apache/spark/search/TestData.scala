package org.apache.spark.search

import java.io.File

import org.apache.spark.sql.{Dataset, SparkSession}

/**
 *
 *
 * @author Pierrick HYMBERT
 */
object TestData {

  case class Person(firstName: String, lastName: String, age: Int)

  def persons = Seq(
    Person("Geoorge", "Michael", 53),
    Person("Bob", "Marley", 37),
    Person("Agnès", "Bartoll", -1))

  case class SecEdgarCompanyInfo(lineNumber: String, companyName: String, companyCIKKey: String)

  case class Company(name: String,
                     domain: String,
                     yearFounded: String,
                     industry: String,
                     sizeRange: String,
                     locality: String,
                     country: String,
                     linkedinUrl: String,
                     currentEmployeeEstimate: String,
                     totalEmployeeEstimate: Long)

  lazy val companiesCSVFilePath: String =
    new File(this.getClass.getResource("/companies.csv").toURI).getAbsolutePath

  /**
   * # Thankfully took from https://www.kaggle.com/peopledatalabssf/free-7-million-company-dataset
   */
  def companiesDS(spark: SparkSession): Dataset[Company] = {
    import spark.implicits._
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(companiesCSVFilePath)
      .withColumnRenamed("year founded", "yearFounded")
      .withColumnRenamed("size range", "sizeRange")
      .withColumnRenamed("linkedin url", "linkedinUrl")
      .withColumnRenamed("current employee estimate", "currentEmployeeEstimate")
      .withColumnRenamed("total employee estimate", "totalEmployeeEstimate")
      .as[Company]
  }

  lazy val companiesEdgarSecCSVFilePath: String =
    new File(this.getClass.getResource("/sec__edgar_company_info.csv").toURI).getAbsolutePath

  /**
   * Thankfully took from https://www.kaggle.com/dattapiy/sec-edgar-companies-list/data
   */
  def companiesEdgarDS(spark: SparkSession): Dataset[SecEdgarCompanyInfo] = {
    import spark.implicits._
    spark.read.option("header", "true")
      .option("inferSchema", "true")
      .csv(companiesEdgarSecCSVFilePath)
      .withColumnRenamed("Line Number", "lineNumber")
      .withColumnRenamed("Company Name", "companyName")
      .withColumnRenamed("Company CIK Key", "companyCIKKey")
      .as[SecEdgarCompanyInfo]
  }
}
