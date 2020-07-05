/**
 * Copyright © 2020 Spark Search (The Spark Search Contributors)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.search.sql

import java.io.{File, Reader}
import java.util.function.Consumer

import org.apache.lucene.analysis.Analyzer.TokenStreamComponents
import org.apache.lucene.analysis.shingle.ShingleFilter
import org.apache.lucene.analysis.standard.StandardTokenizer
import org.apache.lucene.analysis.synonym.{SynonymGraphFilter, SynonymMap}
import org.apache.lucene.analysis.{Analyzer, LowerCaseFilter, TokenStream}
import org.apache.lucene.util.CharsRef
import org.apache.spark.sql.{Dataset, SparkSession}

object TestData {

  case class Company(name: String)

  lazy val companies1: String =
    new File(this.getClass.getResource("/companies-1.csv").toURI).getAbsolutePath


  def companies1DS(spark: SparkSession): Dataset[Company] = {
    import spark.implicits._
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(companies1)
      .as[Company]
  }

  lazy val companies2: String =
    new File(this.getClass.getResource("/companies-2.csv").toURI).getAbsolutePath

  def companies2DS(spark: SparkSession): Dataset[Company] = {
    import spark.implicits._
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(companies2)
      .as[Company]
  }

  class TestCompanyNameAnalyzer extends Analyzer {
    override def createComponents(fieldName: String): TokenStreamComponents = {
      val src = new StandardTokenizer()
      var tok: TokenStream = new LowerCaseFilter(src)
      val builder = new SynonymMap.Builder(true)
      builder.add(new CharsRef("ltd"), new CharsRef("l.t.d"), true)
      builder.add(new CharsRef("ltd"), new CharsRef("limited"), true)
      builder.add(new CharsRef("inc"), new CharsRef("corporation"), true)
      builder.add(new CharsRef("inc"), new CharsRef("corp"), true)
      tok = new SynonymGraphFilter(tok, builder.build(), false)

      val shingle = new ShingleFilter(tok, 2, 2)
      shingle.setOutputUnigrams(true)
      shingle.setOutputUnigramsIfNoShingles(true)
      tok = shingle

      new TokenStreamComponents(new Consumer[Reader] {
        override def accept(r: Reader): Unit = src.setReader(r)
      }, tok)
    }
  }

}
