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
import org.apache.spark.sql.catalyst.util.StringUtils


object SparkRDDRegexBenchmark extends BaseBenchmark("Spark RDD Regex") {

  def main(args: Array[String]): Unit = run()

  override def countNameMatches(companies: RDD[Company], name: String): RDD[(Double, String)] = {
    val re = s".*\\Q${name.toLowerCase}\\E.*"
    companies
      .filter(_.name != null)
      .filter(_.name.toLowerCase.matches(re))
      .map(c => (0, c.name))
  }

  override def joinMatch(companies: RDD[Company], secEdgarCompanies: RDD[SecEdgarCompanyInfo]): RDD[(String, Double, String)] = {
    secEdgarCompanies
      .filter(_.companyName != null)
      .zipWithIndex()
      .map(_.swap)
      .cartesian(companies.filter(_.name != null))
      .map(c => (c._1._1, (c._1._2.companyName, c._2.name)))
      .filter(t => t._2._2.toLowerCase.matches(s".*\\Q${t._2._1.replaceAllLiterally(" ", "\\E\\s+\\Q")}\\E.*"))
      .reduceByKey((c1, _) => c1)
      .map(t => (t._2._1, 0d, t._2._2))
  }
}
