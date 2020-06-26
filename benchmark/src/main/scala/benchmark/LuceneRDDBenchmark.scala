package benchmark

import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.spark.rdd.RDD
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
      .map(r => (r.getAs[Double]("__score__"), r.getAs[String]("name")))
      .sortBy(_._1, ascending = false) // Not sorted by RDD but by partition
  }

  override def joinMatch(companies: RDD[Company], secEdgarCompany: RDD[SecEdgarCompanyInfo]): RDD[(String, Double, String)] = {
    import spark.implicits._

    val luceneRDD = LuceneRDD(companies.toDF(),
      classOf[StandardAnalyzer].getName,
      classOf[StandardAnalyzer].getName,
      "classic")

    val prefixLinker = (company: SecEdgarCompanyInfo) => {
      val skipped = company.companyName.slice(0, 64).replaceAll("([+\\-=&|<>!(){}\\[\\]^\"~*?:/])", "\\\\$1")
      s"name:${"\"" + skipped + "\""}"
    }

    luceneRDD.link(secEdgarCompany, prefixLinker, 1, linkerMethod = "cartesian")
      .filter(_._2.nonEmpty)
      .map(t => (t._1.companyName, t._2.head.getAs[Double]("__score__"), t._2.head.getAs[String]("companyName")))
  }
}
