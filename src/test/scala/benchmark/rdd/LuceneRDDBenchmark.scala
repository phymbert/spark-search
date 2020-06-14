package benchmark.rdd

import org.apache.lucene.analysis.shingle.ShingleAnalyzerWrapper
import org.apache.spark.search.TestData.SecEdgarCompanyInfo
import org.apache.spark.sql.SparkSession
import org.zouzias.spark.lucenerdd.LuceneRDD

object LuceneRDDBenchmark extends BaseBenchmark {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("LuceneRDD").getOrCreate()

    run(spark, (spark, companies, _) => {
      import spark.implicits._

      val luceneRDD = LuceneRDD(companies.toDF(),
        classOf[ShingleAnalyzerWrapper].getName,
        classOf[ShingleAnalyzerWrapper].getName,
        "classic")
      val count = luceneRDD.phraseQuery("name", "ibm~0.8", Int.MaxValue)
        .count
      println(s"Count IBM match: ${count}")
    })

    run(spark, (spark, companies, secEdgarCompany) => {
      import spark.implicits._

      val luceneRDD = LuceneRDD(companies.toDF(),
        classOf[ShingleAnalyzerWrapper].getName,
        classOf[ShingleAnalyzerWrapper].getName,
        "classic")

      val prefixLinker = (company: SecEdgarCompanyInfo) => {
        val skipped = company.companyName.slice(0, 64).replaceAll("([+\\-=&|<>!(){}\\[\\]^\"~*?:/])", "\\\\$1")
        s"name:${"\"" + skipped + "\""}"
      }

      val count = luceneRDD.link(secEdgarCompany, prefixLinker, 1)
        .filter(_._2.nonEmpty)
        .map(t => (t._1.companyName, t._2.head.getAs[Double]("__score__"), t._2.head.getAs[String]("name")))
        .count
      println(s"Link count: $count")
    })
    spark.stop()
  }
}
