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

import org.apache.spark.search.rdd._
import org.apache.spark.sql.{Dataset, Encoder, Encoders}

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
  def defaultDatasetOpts[T]: SearchRDDOptions[T] = SearchRDDOptions
    .builder()
    .build()

  /**
   * Allow search records rdd transformation to DS.
   */
  implicit def searchRecordEncoder[T <: Product : TypeTag]: Encoder[SearchRecord[T]] = Encoders.product[SearchRecord[T]]

  /**
   * Allow match record rdd transformation to DS.
   */
  implicit def matchingEncoder[T <: Product : TypeTag, S <: Product : TypeTag]: Encoder[Match[T, S]] = Encoders.product[Match[T, S]]

  /**
   * Add search feature to DS
   */
  implicit def datasetWithSearch[T: ClassTag](dataset: Dataset[T]): DatasetWithSearch[T] = new DatasetWithSearch[T](dataset)

}
