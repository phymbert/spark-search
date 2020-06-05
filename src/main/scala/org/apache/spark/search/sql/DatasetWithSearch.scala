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
import org.apache.spark.search.rdd.{SearchRDDOptions, SearchRecord, _}
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.{Dataset, Encoder, Row}

import scala.reflect.ClassTag

/**
 * Dataset with search features.
 *
 * @author Pierrick HYMBERT
 */
class DatasetWithSearch[T: ClassTag](dataset: Dataset[T]) extends Serializable {

  /**
   * [[org.apache.spark.search.rdd.SearchRDD#count(org.apache.lucene.search.Query)]]
   */
  def count(query: String, opts: SearchRDDOptions[T] = defaultDatasetOpts): Long =
    searchRDD(opts).count(query)

  /**
   * [[org.apache.spark.search.rdd.SearchRDD#searchList(org.apache.lucene.search.Query, int, double)]]
   */
  def searchList(query: String,
                 topK: Int = Int.MaxValue,
                 minScore: Double = 0,
                 opts: SearchRDDOptions[T] = defaultDatasetOpts): Array[SearchRecord[T]] =
    searchRDD(opts).searchList(query, topK, minScore)

  /**
   * [[org.apache.spark.search.rdd.SearchRDD#search(org.apache.lucene.search.Query, int, double)]]
   */
  def search(query: String,
             topKByPartition: Int = Int.MaxValue,
             minScore: Double = 0,
             opts: SearchRDDOptions[T] = defaultDatasetOpts)(implicit enc: Encoder[SearchRecord[T]]): Dataset[SearchRecord[T]] /*Dataset[SearchRecord[T]]*/ = {

    val _searchRecordToRow = searchRecordToRow()
    val rdd = searchRDD(opts)
      .search(query, topKByPartition, minScore)
      .map(_searchRecordToRow)

    dataset.sqlContext.createDataFrame(rdd, enc.schema).as[SearchRecord[T]]
  }

  /**
   * Joins the input RDD against this one and returns matching hits.
   *
   * [[org.apache.spark.search.rdd.SearchRDD#searchJoin(org.apache.spark.rdd.RDD, scala.Function1, int, double)]]
   */
  def searchJoin[S: ClassTag](ds: Dataset[S],
                              queryBuilder: S => String,
                              topK: Int = Int.MaxValue,
                              minScore: Double = 0,
                              opts: SearchRDDOptions[T] = defaultDatasetOpts)(implicit enc: Encoder[Match[S, T]]): Dataset[Match[S, T]] = {
    // FIXME support row natively
    val _searchRecordToRow = searchRecordToRow()
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
                     opts: SearchRDDOptions[T])(implicit enc: Encoder[T]): Dataset[T] = {
    val rdd = searchRDD(opts)
      .searchDropDuplicates(queryBuilder, topK, minScore, numPartitions)
      .map(asRow)

    dataset.sqlContext.createDataFrame(rdd, enc.schema).as[T]
  }

  private def asRow[H](d: H) = {
    (d match {
      case r: Row => r
      case _ => new GenericRow(d match {
        case p: Product => p.productIterator.toSeq.toArray
        // FIXME Support Bean
        // GenCode for others case _ => enc.asInstanceOf[ExpressionEncoder].deserializer.genCode()
      })
    }).asInstanceOf[Row]
  }

  def searchRDD: SearchRDD[T] = searchRDD(defaultOpts)

  /**
   * @param opts Search options
   * @return Dependent RDD with configurable search features
   */
  def searchRDD(opts: SearchRDDOptions[T]): SearchRDD[T] =
    new SearchRDD[T](dataset.rdd, opts).cache

  private def searchRecordToRow(): SearchRecord[T] => Row = (sr: SearchRecord[T]) =>
    new GenericRow(Array(sr.id, sr.partitionIndex, sr.score, sr.shardIndex, asRow(sr.source))).asInstanceOf[Row]

}
