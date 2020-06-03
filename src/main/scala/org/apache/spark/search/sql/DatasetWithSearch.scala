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
import org.apache.spark.sql.types.{StructField, StructType}
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
   * [[org.apache.spark.search.rdd.SearchRDD#searchList(SearchQuery, int, float)]]
   */
  def searchList(query: SearchQuery,
                 topK: Int = Int.MaxValue,
                 minScore: Float = 0,
                 opts: SearchRDDOptions[T] = defaultDatasetOpts): Array[SearchRecord[T]] =
    searchRDD(opts).searchList(query, topK, minScore)

  /**
   * [[org.apache.spark.search.rdd.SearchRDD#search(SearchQuery, int, float)]]
   */
  def search(query: SearchQuery,
             topKByPartition: Int = Int.MaxValue,
             minScore: Float = 0,
             opts: SearchRDDOptions[T] = defaultDatasetOpts)(implicit enc: Encoder[SearchRecord[T]], enc2: Encoder[T]): Dataset[SearchRecord[T]] /*Dataset[SearchRecord[T]]*/ = {
    val datasetSchema = enc2.schema
    val schema = StructType(enc.schema.slice(0, enc.schema.length - 1) ++ Seq(StructField("source", datasetSchema)))

    val rdd = searchRDD(opts)
      .search(query, topKByPartition, minScore)
      .map(b => new GenericRow(Array(b.id, b.partitionIndex, b.score, b.shardIndex, new GenericRow(b.source match {
        case p: Product => p.productIterator.toSeq.toArray
        // FIXME Support Bean
        // GenCode for others case _ => enc.asInstanceOf[ExpressionEncoder].deserializer.genCode()
      }))).asInstanceOf[Row])

    dataset.sqlContext.createDataFrame(rdd, schema).as[SearchRecord[T]]
  }

  def searchRDD: SearchRDD[T] = searchRDD(defaultOpts)

  /**
   * @param opts Search options
   * @return Dependent RDD with configurable search features
   */
  def searchRDD(opts: SearchRDDOptions[T]): SearchRDD[T] =
    new SearchRDD[T](dataset.rdd, opts).cache

}
