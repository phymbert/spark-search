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

import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.zouzias.spark.lucenerdd.LuceneRDD

object LuceneRDDBenchmark extends BaseBenchmark("LuceneRDD") {
  def main(args: Array[String]): Unit = run()

  override def countNameMatches(companies: RDD[Company], name: String): RDD[(Double, String)] = {
    import spark.implicits._
    val luceneRDD = LuceneRDD(companies.toDF(),
      classOf[StandardAnalyzer].getName,
      classOf[StandardAnalyzer].getName,
      "classic")
    luceneRDD.query(s"name:${name}", Int.MaxValue)
      .map(r => (r.getAs[Float]("__score__").toDouble, r.getAs[String]("name")))
      .sortBy(_._1, ascending = false) // Not sorted by RDD but by partition
  }

  override def joinMatch(spark: SparkSession, companies: RDD[Company], secEdgarCompany: RDD[SecEdgarCompanyInfo]): RDD[(String, Double, String)] = {
    import spark.implicits._

    val luceneRDD = LuceneRDD(companies.toDF(),
      classOf[StandardAnalyzer].getName,
      classOf[StandardAnalyzer].getName,
      "classic")

    val prefixLinker = (spark: SparkSession, company: SecEdgarCompanyInfo) => {
      val skipped = company.companyName.slice(0, 64).replaceAll("([+\\-=&|<>!(){}\\[\\]^\"~*?:/])", "\\\\$1")
      s"name:${"\"" + skipped + "\""}"
    }

    luceneRDD.link(secEdgarCompany, prefixLinker, 1, linkerMethod = "cartesian")
      .filter(_._2.nonEmpty)
      .map(t => (t._1.companyName, t._2.head.getAs[Double]("__score__"), t._2.head.getAs[String]("name")))
  }
}
