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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.util.StringUtils


object SparkRDDRegexBenchmark extends BaseBenchmark {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Spark RDD Regex").getOrCreate()
    run(spark, (_, companies, _) => {
      val count = companies
        .filter(_.name != null)
        .filter(_.name.toLowerCase.matches(".*ibm.*")).count()
      println(s"Count IBM match: ${count}")
    })

    run(spark, (_, companies, secEdgarCompany) => {

      val count = companies
        .filter(_.name != null)
        .zipWithIndex().map(_.swap)
        .cartesian(secEdgarCompany.filter(_.companyName != null).map(Iterator(_)))
        .map(t => (t._1._1, (t._1._2, t._2)))
        .filter(t => t._2._1.name.toLowerCase.matches("^.*" +
          StringUtils.escapeLikeRegex(t._2._2.next.companyName.slice(0, 64)
            .toLowerCase.replaceAllLiterally(" ", "\\s+")) + ".*$"))
        .reduceByKey((c1, c2) => (c1._1, c1._2 ++ c2._2))
        .count
      println(s"Link count: $count")
    })
    spark.stop()
  }
}
