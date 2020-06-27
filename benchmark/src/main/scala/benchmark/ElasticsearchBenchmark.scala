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

import org.apache.http.client.methods.HttpDelete
import org.apache.http.impl.client.DefaultHttpClient
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.elasticsearch.spark._

object ElasticsearchBenchmark extends BaseBenchmark("Elasticsearch") {

  val esSQLSource = "org.elasticsearch.spark.sql"

  def main(args: Array[String]): Unit = run()

  override def countNameMatches(companies: RDD[Company], name: String): RDD[(Double, String)] = {
    import spark.implicits._
    clearES()
    companies.saveToEs("companies") // FIXME maybe adjust replica/shard preferences
    spark.read.format(esSQLSource)
      .load("companies")
      .filter($"name".equalTo(name))
      .as[Company]
      .map(c => (0d, c.name)) // FIXME no score ?
      .rdd
  }

  override def joinMatch(companies: RDD[Company], secEdgarCompanies: RDD[SecEdgarCompanyInfo]): RDD[(String, Double, String)] = {
    clearES()
    import spark.implicits._
    companies.saveToEs("companies")
    secEdgarCompanies.saveToEs("secEdgarCompanies")

    val companiesES = spark.read.format(esSQLSource)
      .load("companies")
      .as[Company]

    val secEdgarCompaniesES = spark.read.format(esSQLSource)
      .load("secEdgarCompanies")
      .as[SecEdgarCompanyInfo]

    companiesES.join(secEdgarCompaniesES, $"name".equalTo($"companyName")) //FIXME check pushdown filters
      .select($"companyName", lit(0d), $"name")
      .distinct
      .as[(String, Double, String)]
      .rdd
  }

  private def clearES() = {
    spark.conf.set("es.nodes", spark.conf.get("spark.es.nodes"))
    spark.conf.set("es.port", spark.conf.get("spark.es.port"))
    spark.conf.set("es.nodes.wan.only", value = true)

    val deleteIndices = new HttpDelete(s"https://${spark.conf.get("es.nodes")}:${spark.conf.get("es.port")}/companies,secEdgarCompanies")
    (new DefaultHttpClient).execute(deleteIndices)
  }
}
