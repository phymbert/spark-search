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

package benchmark.rdd

import org.apache.lucene.analysis.shingle.ShingleAnalyzerWrapper
import org.apache.lucene.util.QueryBuilder
import org.apache.spark.search.TestData._
import org.apache.spark.search.rdd._
import org.apache.spark.search.{SearchOptions, _}
import org.apache.spark.sql.SparkSession

object SearchRDDBenchmark extends BaseBenchmark {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SearchRDD").getOrCreate()
    run(spark, (_, companies, _) => {
      val opts = SearchOptions
        .builder[Company]
        .analyzer(classOf[ShingleAnalyzerWrapper]).build

      val count = companies.searchRDD(opts)
        .count("name:ibm~0.8")
      println(s"Count IBM match: ${count}")
    })

    run(spark, (_, companies, secEdgarCompany) => {
      val opts = SearchOptions
        .builder[Company]
        .analyzer(classOf[ShingleAnalyzerWrapper]).build

      val count = companies.searchRDD(opts)
        .searchQueryJoin(secEdgarCompany, queryBuilder[SecEdgarCompanyInfo]((c: SecEdgarCompanyInfo, lqb: QueryBuilder) =>
          lqb.createPhraseQuery("name", c.companyName.slice(0, 64)), opts), 1, 0d)
        .filter(_.hits.nonEmpty).map(m => (m.doc.companyName, m.hits.head.score, m.hits.head.source.name)).count
      println(s"Link count: $count")
    })
    spark.stop()
  }
}
