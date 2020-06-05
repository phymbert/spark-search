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
package org.apache.spark.search.sql

import org.apache.lucene.search.Query
import org.apache.spark.search.{SearchOptions, _}
import org.apache.spark.search.rdd._
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.{Dataset, Encoder, Row}

import scala.reflect.ClassTag

/**
 * Dataset with search features.
 *
 * FIXME: maybe need to deal only with Rows, instead of reflection with product/beans, just use encoder, and let spark does the conversions
 *
 * @author Pierrick HYMBERT
 */
class DatasetWithSearch[T: ClassTag](dataset: Dataset[T]) extends Serializable {

  /**
   * [[org.apache.spark.search.rdd.SearchRDD#count(org.apache.lucene.search.Query)]]
   */
  def count(query: String): Long =
    count(query, defaultDatasetOpts[T])

  /**
   * [[org.apache.spark.search.rdd.SearchRDD#count(org.apache.lucene.search.Query)]]
   */
  def count(query: String, opts: SearchOptions[T]): Long =
    searchRDD(opts).count(query)

  /**
   * [[org.apache.spark.search.rdd.SearchRDD#searchList(org.apache.lucene.search.Query, int, double)]]
   */
  def searchList(query: String): Array[SearchRecord[T]] =
    searchList(query, Int.MaxValue)

  /**
   * [[org.apache.spark.search.rdd.SearchRDD#searchList(org.apache.lucene.search.Query, int, double)]]
   */
  def searchList(query: String,
                 topK: Int): Array[SearchRecord[T]] =
    searchList(query, topK, 0)

  /**
   * [[org.apache.spark.search.rdd.SearchRDD#searchList(org.apache.lucene.search.Query, int, double)]]
   */
  def searchList(query: String,
                 topK: Int,
                 minScore: Double): Array[SearchRecord[T]] =
    searchList(query, topK, minScore, defaultDatasetOpts[T])

  /**
   * [[org.apache.spark.search.rdd.SearchRDD#searchList(org.apache.lucene.search.Query, int, double)]]
   */
  def searchList(query: String,
                 topK: Int,
                 minScore: Double,
                 opts: SearchOptions[T]): Array[SearchRecord[T]] =
    searchRDD(opts).searchList(query, topK, minScore)

  /**
   * [[org.apache.spark.search.rdd.SearchRDD#search(org.apache.lucene.search.Query, int, double)]]
   */
  def search(query: String,
             topKByPartition: Int = Int.MaxValue,
             minScore: Double = 0,
             opts: SearchOptions[T] = defaultDatasetOpts[T])(implicit enc: Encoder[SearchRecord[T]]): Dataset[SearchRecord[T]] = {

    val _searchRecordToRow = searchRecordToRow[T]()
    val rdd = searchRDD(opts)
      .search(query, topKByPartition, minScore)
      .map(_searchRecordToRow)

    dataset.sqlContext.createDataFrame(rdd, enc.schema).as[SearchRecord[T]]
  }

  /**
   * Joins the input DS against this one and returns matching hits.
   *
   * [[org.apache.spark.search.rdd.SearchRDD#searchJoin(org.apache.spark.rdd.RDD, scala.Function1, int, double)]]
   */
  def searchJoin[S: ClassTag](ds: Dataset[S],
                              queryBuilder: S => String,
                              topK: Int = Int.MaxValue,
                              minScore: Double = 0,
                              opts: SearchOptions[T] = defaultDatasetOpts[T])(implicit enc: Encoder[Match[S, T]]): Dataset[Match[S, T]] = {
    val _searchRecordToRow = searchRecordToRow[T]()
    val rdd = searchRDD(opts)
      .searchQueryJoin(ds.rdd, queryStringBuilder(queryBuilder), topK, minScore)
      .map(m => new GenericRow(Array[Any](asRow(m.doc), m.hits.map(_searchRecordToRow))).asInstanceOf[Row])

    dataset.sqlContext.createDataFrame(rdd, enc.schema).as[Match[S, T]]
  }

  /**
   * Drop duplicates records by applying lookup for matching hits of the query against this RDD.
   */
  def searchDropDuplicates(queryBuilder: T => Query)(implicit enc: Encoder[T]): Dataset[T] =
    searchDropDuplicates(queryBuilder, Int.MaxValue)


  /**
   * Drop duplicates records by applying lookup for matching hits of the query against this RDD.
   */
  def searchDropDuplicates(queryBuilder: T => Query, topK: Int)(implicit enc: Encoder[T]): Dataset[T] =
    searchDropDuplicates(queryBuilder, topK, 0)

  /**
   * Drop duplicates records by applying lookup for matching hits of the query against this RDD.
   */
  def searchDropDuplicates(queryBuilder: T => Query, topK: Int, minScore: Double)(implicit enc: Encoder[T]): Dataset[T] =
    searchDropDuplicates(queryBuilder, topK, minScore, dataset.rdd.getNumPartitions)

  /**
   * Drop duplicates records by applying lookup for matching hits of the query against this RDD.
   */
  def searchDropDuplicates(queryBuilder: T => Query,
                           topK: Int,
                           minScore: Double,
                           numPartitions: Int)(implicit enc: Encoder[T]): Dataset[T] =
    searchDropDuplicates(queryBuilder, topK, minScore, numPartitions, defaultDatasetOpts[T])

  /**
   * Drop duplicates records by applying lookup for matching hits of the query against this RDD.
   */
  def searchDropDuplicates(queryBuilder: T => Query,
                           topK: Int,
                           minScore: Double,
                           numPartitions: Int,
                           opts: SearchOptions[T])(implicit enc: Encoder[T]): Dataset[T] = {
    val rdd = searchRDD(opts)
      .searchDropDuplicates(queryBuilder, topK, minScore, numPartitions)
      .map(asRow)

    dataset.sqlContext.createDataFrame(rdd, enc.schema).as[T]
  }

  /**
   * @param opts Search options
   * @return Dependent RDD with configurable search features
   */
  def searchRDD(opts: SearchOptions[T]): SearchRDD[T] =
    new SearchRDD[T](dataset.rdd, opts).cache

  private[sql] def searchRDD: SearchRDD[T] = searchRDD(defaultDatasetOpts[T])
}
