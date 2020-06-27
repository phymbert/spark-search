package org.apache.spark.search

import java.io.File

import org.apache.lucene.analysis.Analyzer.TokenStreamComponents
import org.apache.lucene.analysis.ngram.NGramTokenizer
import org.apache.lucene.analysis.shingle.ShingleFilter
import org.apache.lucene.analysis.standard.StandardTokenizer
import org.apache.lucene.analysis.{Analyzer, LowerCaseFilter, TokenStream}
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
                     totalEmployeeEstimate: String)

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

  class TestPersonAnalyzer extends Analyzer {
    override def createComponents(fieldName: String): TokenStreamComponents = {
      val src = new NGramTokenizer(1, 3)
      new TokenStreamComponents(r => src.setReader(r), src)
    }
  }

}
