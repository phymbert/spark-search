package org.apache.spark.search.sql

import org.apache.spark.search.rdd.SearchRDD
import org.apache.spark.search.{SearchOptions, queryStringBuilder}
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.types.{ArrayType, DataTypes, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row}

/**
 * DataFrame with search features.
 *
 * @author Pierrick HYMBERT
 */
class DataFrameWithSearch(df: DataFrame) {

  /**
   * [[org.apache.spark.search.rdd.SearchRDD#count(org.apache.lucene.search.Query)]]
   */
  def count(query: String, opts: SearchOptions[Row] = defaultDataFrameOpts(df.schema)): Long =
    searchRDD(opts).count(query)

  /**
   * [[org.apache.spark.search.rdd.SearchRDD#searchList(org.apache.lucene.search.Query, int, double)]]
   */
  def searchList(query: String,
                 topK: Int = Int.MaxValue,
                 minScore: Double = 0,
                 opts: SearchOptions[Row] = defaultDataFrameOpts(df.schema)): Array[Row] =
    searchRDD(opts).searchList(query, topK, minScore).map(searchRecordToRow[Row]())

  /**
   * [[org.apache.spark.search.rdd.SearchRDD#search(org.apache.lucene.search.Query, int, double)]]
   */
  def search(query: String,
             topKByPartition: Int = Int.MaxValue,
             minScore: Double = 0,
             opts: SearchOptions[Row] = defaultDataFrameOpts(df.schema)): DataFrame = {

    val rdd = searchRDD(opts)
      .search(query, topKByPartition, minScore)
      .map(sr => {
        val r = searchRecordToRow[Row]()(sr)
        r
      })

    df.sqlContext.createDataFrame(rdd, searchRecordSchema)
  }

  /**
   * Joins the input DataFrame against this one and returns matching hits.
   *
   * [[org.apache.spark.search.rdd.SearchRDD#searchJoin(org.apache.spark.rdd.RDD, scala.Function1, int, double)]]
   */
  def searchJoin(otherDF: DataFrame,
                 queryBuilder: Row => String,
                 topK: Int = Int.MaxValue,
                 minScore: Double = 0,
                 opts: SearchOptions[Row] = defaultDataFrameOpts(df.schema)): Dataset[Row] = {
    val _searchRecordToRow = searchRecordToRow[Row]()
    val rdd = searchRDD(opts)
      .searchQueryJoin(otherDF.rdd, queryStringBuilder(queryBuilder), topK, minScore)
      .map(m => new GenericRow(Array[Any](asRow(m.doc), m.hits.map(_searchRecordToRow))).asInstanceOf[Row])

    otherDF.sqlContext.createDataFrame(rdd, matchRecordSchema(otherDF.schema))
  }

  private val searchRecordSchema = {
    new StructType(Array(
      StructField("id", DataTypes.LongType),
      StructField("partitionIndex", DataTypes.LongType),
      StructField("score", DataTypes.DoubleType),
      StructField("shardIndex", DataTypes.LongType),
      StructField("source", df.schema)))
  }

  private def matchRecordSchema(matchQuerySchema: StructType) =
    new StructType(Array(
      StructField("doc", matchQuerySchema),
      StructField("hits", ArrayType(searchRecordSchema))))

  private[sql] def searchRDD: SearchRDD[Row] = searchRDD(defaultDataFrameOpts(df.schema))

  /**
   * @param opts Search options
   * @return Dependent RDD with configurable search features
   */
  def searchRDD(opts: SearchOptions[Row]): SearchRDD[Row] =
    new SearchRDD[Row](df.rdd, opts).cache
}
