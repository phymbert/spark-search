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

package org.apache.spark.search

import org.apache.lucene.search.Query
import org.apache.spark.search
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.types.StructType

import scala.language.implicitConversions
import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

/**
 * Search SQL package provides search features to spark [[org.apache.spark.sql.Dataset]].
 *
 * @author Pierrick HYMBERT
 */
package object sql {

  /**
   * Default search options.
   */
  def defaultDatasetOpts[T: ClassTag]()(implicit classTag: ClassTag[T]): SearchOptions[T] =
    SearchOptions.builder().build()

  /**
   * Default search options.
   */
  def defaultDataFrameOpts(schema: StructType): SearchOptions[Row] =
    SearchOptions.builder()
      .read((readOptsBuilder: ReaderOptions.Builder[Row]) => readOptsBuilder.documentConverter(new DocumentRowConverter(schema)))
      .index((indexOptsBuilder: IndexationOptions.Builder[Row]) => indexOptsBuilder.documentUpdater(new DocumentRowUpdater()))
      .build()

  /**
   * Default query builder.
   */
  def defaultQueryBuilder[T: ClassTag]()(implicit enc: Encoder[T]): T => Query = search.defaultQueryBuilder[T]()

  /**
   * Allow search record rdd transformation to Row.
   */
  implicit def searchRecordEncoder[T <: Product : TypeTag]: Encoder[SearchRecord[T]] = Encoders.product[SearchRecord[T]]

  /**
   * Allow match record rdd transformation to Row.
   */
  implicit def matchingEncoder[T <: Product : TypeTag, S <: Product : TypeTag]: Encoder[Match[T, S]] = Encoders.product[Match[T, S]]

  /**
   * Add search feature to DS
   */
  implicit def datasetWithSearch[T: ClassTag](dataset: Dataset[T]): DatasetWithSearch[T] = new DatasetWithSearch[T](dataset)

  /**
   * Add search feature to DF
   */
  implicit def dataFrameWithSearch(dataset: DataFrame): DataFrameWithSearch = new DataFrameWithSearch(dataset)

  private[sql] def searchRecordToRow[T](): SearchRecord[T] => Row = (sr: SearchRecord[T]) =>
    new GenericRow(Array(sr.id, sr.partitionIndex, sr.score, sr.shardIndex, asRow(sr.source))).asInstanceOf[Row]

  private[sql] def asRow[H](d: H) = {
    (d match {
      case r: Row => r
      case _ => new GenericRow(d match {
        case p: Product => p.productIterator.toSeq.toArray
        // FIXME Support Bean
        // GenCode for others case _ => enc.asInstanceOf[ExpressionEncoder].deserializer.genCode()
      })
    }).asInstanceOf[Row]
  }
}
