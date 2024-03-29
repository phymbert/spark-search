/*
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

import org.apache.http.client.methods.HttpDelete
import org.apache.http.impl.client.DefaultHttpClient
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.elasticsearch.spark._

object ElasticsearchBenchmark extends BaseBenchmark("Elasticsearch") {

  val esSQLSource = "org.elasticsearch.spark.sql"

  val esOpts = Map("es.nodes.wan.only" -> "true")

  def main(args: Array[String]): Unit = run()

  override def countNameMatches(companies: RDD[Company], name: String): RDD[(Double, String)] = {
    import spark.implicits._
    clearES()
    companies.saveToEs("companies", esOpts) // FIXME maybe adjust replica/shard preferences
    spark.read.format(esSQLSource)
      .option("es.nodes.wan.only", "true")
      .load("companies")
      .filter($"name".equalTo(name))
      .as[Company]
      .map(c => (0d, c.name)) // FIXME no score ?
      .rdd
  }

  override def joinMatch(companies: RDD[Company], secEdgarCompanies: RDD[SecEdgarCompanyInfo]): RDD[(String, Double, String)] = {
    clearES()
    import spark.implicits._
    companies.saveToEs("companies", esOpts)
    secEdgarCompanies.saveToEs("sec_edgar_companies", esOpts)

    val companiesES = spark.read
      .format(esSQLSource)
      .option("es.nodes.wan.only", "true")
      .load("companies")
      .as[Company]

    val secEdgarCompaniesES = spark.read.format(esSQLSource)
      .option("es.nodes.wan.only", "true")
      .load("sec_edgar_companies")
      .as[SecEdgarCompanyInfo]

    companiesES.join(secEdgarCompaniesES, $"name".equalTo($"companyName")) //FIXME check pushdown filters
      .select($"companyName", lit(0d), $"name")
      .distinct
      .as[(String, Double, String)]
      .rdd
  }

  private def clearES() = {
    spark.conf.set("es.nodes", spark.conf.get("spark.es.nodes"))
    spark.conf.set("es.port", "80")

    val deleteIndices = new HttpDelete(s"http://${spark.conf.get("es.nodes")}/companies,sec_edgar_companies")
    println(s"Deleting indices ${deleteIndices}...")
    val resp = (new DefaultHttpClient).execute(deleteIndices)
    println(s"Indices deleted: ${resp}")
  }
}
