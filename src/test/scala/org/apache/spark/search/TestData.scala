package org.apache.spark.search

import java.io.{File, Reader}
import java.util
import java.util.function.Consumer

import org.apache.lucene.analysis.Analyzer.TokenStreamComponents
import org.apache.lucene.analysis.core.FlattenGraphFilter
import org.apache.lucene.analysis.miscellaneous.WordDelimiterGraphFilterFactory
import org.apache.lucene.analysis.shingle.ShingleFilterFactory
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

  class TestAnalyzer extends Analyzer {
    lazy val _tokenStreamComponents = {
      val src = new StandardTokenizer
      var tok: TokenStream = new LowerCaseFilter(src)
      val params = new util.HashMap[String, String]()
      params.put("generateWordParts", "1")
      params.put("catenateWords", "1")
      params.put("generateNumberParts", "1")
      params.put("catenateNumbers", "1")
      params.put("catenateAll", "1")
      params.put("preserveOriginal", "1")
      tok = new WordDelimiterGraphFilterFactory(params).create(tok)
      tok = new FlattenGraphFilter(tok)
      params.clear()
      params.put("minShingleSize", "2")
      params.put("maxShingleSize", "4")
      params.put("outputUnigrams", "true")
      params.put("outputUnigramsIfNoShingles", "true")
      tok = new ShingleFilterFactory(params).create(tok)

      new TokenStreamComponents(new Consumer[Reader] {
        override def accept(r: Reader): Unit = src.setReader(r)
      }, tok)
    }

    override def createComponents(fieldName: String): Analyzer.TokenStreamComponents = _tokenStreamComponents
  }

}
