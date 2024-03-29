/*
 * Copyright © 2020 Spark Search (The Spark Search Contributors)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.search

import org.apache.lucene.search.Query
import org.apache.spark.search
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.types.DoubleType

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
   * Score column name.
   */
  val SCORE: String = "__score__"

  private[sql] val scoreAttribute: Attribute = AttributeReference(SCORE, DoubleType, nullable = false)()

  /**
   * Score of the hit in the search request.
   */
  def score(): Column = new Column(ScoreExpression())

  /**
   * Default query builder.
   */
  def defaultQueryBuilder[S: ClassTag](implicit enc: Encoder[S]): S => Query = search.defaultQueryBuilder[S]()

  /**
   * Add search feature to column.
   */
  implicit def columnWithSearch(col: Column): ColumnWithSearch = new ColumnWithSearch(col)

  /**
   * Allow search record rdd transformation to Row.
   */
  implicit def searchRecordEncoder[S <: Product : TypeTag](implicit enc: Encoder[S]): Encoder[SearchRecord[S]] = Encoders.product[SearchRecord[S]]
}
