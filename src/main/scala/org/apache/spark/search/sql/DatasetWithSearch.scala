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

import org.apache.spark.search.rdd.{SearchRDDOptions, SearchRecord, _}
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.{Dataset, Encoder, Row}

import scala.reflect.ClassTag

/**
 * Dataset with search features.
 *
 * @author Pierrick HYMBERT
 */
class DatasetWithSearch[T: ClassTag](dataset: Dataset[T]) {

  /**
   * [[org.apache.spark.search.rdd.SearchRDD#count(SearchQuery)]]
   */
  def count(query: SearchQuery, opts: SearchRDDOptions[T] = defaultDatasetOpts): Long =
    searchRDD(opts).count(query)

  /**
   * [[org.apache.spark.search.rdd.SearchRDD#searchList(SearchQuery, int, Double)]]
   */
  def searchList(query: SearchQuery,
                 topK: Int = Int.MaxValue,
                 minScore: Double = 0,
                 opts: SearchRDDOptions[T] = defaultDatasetOpts): Array[SearchRecord[T]] =
    searchRDD(opts).searchList(query, topK, minScore)

  /**
   * [[org.apache.spark.search.rdd.SearchRDD#search(SearchQuery, int, Double)]]
   */
  def search(query: SearchQuery,
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
   */
  def searchJoin[S](ds: Dataset[S],
                    queryBuilder: S => SearchQuery,
                    topK: Int = Int.MaxValue,
                    minScore: Double = 0,
                    opts: SearchRDDOptions[T] = defaultDatasetOpts)(implicit enc: Encoder[Match[S, T]]): Dataset[Match[S, T]] = {
    val _searchRecordToRow = searchRecordToRow()
    val rdd = searchRDD(opts)
      .searchJoin(ds.rdd, queryBuilder, topK, minScore)
      .map(m => new GenericRow(Array[Any](new GenericRow(m.doc match {
        case p: Product => p.productIterator.toSeq.toArray
        // FIXME Support Bean
        // GenCode for others case _ => enc.asInstanceOf[ExpressionEncoder].deserializer.genCode()
      }), m.hits.map(_searchRecordToRow).toArray)).asInstanceOf[Row])

    dataset.sqlContext.createDataFrame(rdd, enc.schema).as[Match[S, T]]
  }

  def searchRDD: SearchRDD[T] = searchRDD(defaultOpts)

  /**
   * @param opts Search options
   * @return Dependent RDD with configurable search features
   */
  def searchRDD(opts: SearchRDDOptions[T]): SearchRDD[T] =
    new SearchRDD[T](dataset.rdd, opts).cache

  private def searchRecordToRow(): SearchRecord[T] => Row = (sr: SearchRecord[T]) =>
    new GenericRow(Array(sr.id, sr.partitionIndex, sr.score, sr.shardIndex, new GenericRow(sr.source match {
      case p: Product => p.productIterator.toSeq.toArray
      // FIXME Support Bean
      // GenCode for others case _ => enc.asInstanceOf[ExpressionEncoder].deserializer.genCode()
    }))).asInstanceOf[Row]

}
