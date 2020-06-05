package org.apache.spark.search.sql

import java.io.{File, FileInputStream, FileOutputStream}
import java.net.URL
import java.util.zip.ZipInputStream

import org.apache.lucene.analysis.en.EnglishAnalyzer
import org.apache.spark.search._
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

import scala.sys.process._

/**
 * Spark Search DataFrame examples.
 */
object SearchDataframeExamples {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("Spark Search Examples").getOrCreate()
    import spark.implicits._

    // Download sentiment database
    println("Downloading stanford sentiment tree bank file...")
    val sentimentZipFile = File.createTempFile("stanfordSentimentTreebank", ".zip")
    sentimentZipFile.deleteOnExit()
    new URL("https://nlp.stanford.edu/~socherr/stanfordSentimentTreebank.zip") #> sentimentZipFile !!
    val sentimentSentencesFile = File.createTempFile("stanfordSentimentTreebank", ".tsv")
    sentimentSentencesFile.deleteOnExit()
    val fis = new FileInputStream(sentimentZipFile)
    val zis = new ZipInputStream(fis)
    Stream.continually(zis.getNextEntry)
      .takeWhile(e => e != null)
      .filter(e => e.getName.equals("stanfordSentimentTreebank/datasetSentences.txt"))
      .foreach { e =>
        val fout = new FileOutputStream(sentimentSentencesFile)
        val buffer = new Array[Byte](1024)
        Stream.continually(zis.read(buffer)).takeWhile(_ != -1).foreach(fout.write(buffer, 0, _))
      }

    println("Sentiments tree bank downloaded, loading...")
    val sentiments = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", "\t").csv(sentimentSentencesFile.getAbsolutePath)
      .cache.repartition(4).toDF
    sentiments.show()

    // Search SQL API
    // import org.apache.spark.search.sql._ to implicitly enhance DataFrame with search features

    // Count positive sentiments: indexation + count matched doc
    val happySentiments = sentiments.count("sentence:happy OR sentence:best or sentence:good")
    println(s"${happySentiments} positive sentence :)")

    // Search join sentiments
    val sentimentsKeywords = spark.sparkContext.parallelize(Seq("good", "bad", "happy", "sad", "best", "worst", "amazing"))
      .toDF("sentimentKeyword")

    val sentimentsPerKeyWord = sentiments.searchJoin(sentimentsKeywords, (row: Row) => s"sentence:${row.getString(0)}")

    sentimentsPerKeyWord.write.mode(SaveMode.Overwrite).json("reviews-matched-df")

    spark.close()
  }
}
