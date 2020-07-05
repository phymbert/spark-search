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
package benchmark

import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.util.QueryBuilder
import org.apache.spark.rdd.RDD
import org.apache.spark.search.rdd._
import org.apache.spark.search.{SearchOptions, _}

object SearchRDDBenchmark extends BaseBenchmark("SearchRDD") {

  def main(args: Array[String]): Unit = run()

  override def countNameMatches(companies: RDD[Company], name: String): RDD[(Double, String)] = {
    val opts = SearchOptions
      .builder[Company]
      .analyzer(classOf[StandardAnalyzer]).build

    companies.searchRDD(opts).search(s"name:${name}").map(sr => (sr.score, sr.source.name))
  }

  override def joinMatch(companies: RDD[Company], secEdgarCompanies: RDD[SecEdgarCompanyInfo]): RDD[(String, Double, String)] = {
    val opts = SearchOptions
      .builder[Company]
      .analyzer(classOf[StandardAnalyzer]).build

    companies.searchRDD(opts)
      .searchQueryJoin(secEdgarCompanies,
        queryBuilder[SecEdgarCompanyInfo]((c: SecEdgarCompanyInfo, lqb: QueryBuilder) =>
          lqb.createPhraseQuery("name", c.companyName.slice(0, 64)), opts), 1, 0d)
      .filter(_.hits.nonEmpty).map(m => (m.doc.companyName, m.hits.head.score, m.hits.head.source.name))

  }
}
