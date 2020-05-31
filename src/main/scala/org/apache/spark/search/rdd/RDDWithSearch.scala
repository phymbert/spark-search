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

package org.apache.spark.search.rdd

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
 * Add search fatures to rdd.
 */
private[rdd] class RDDWithSearch[T: ClassTag](val rdd: RDD[T]) {
  def search: SearchRDD[T] = search(SearchRDDOptions.defaultOptions())

  def search(opts: SearchRDDOptions[T]) = new SearchRDD[T](rdd, opts)
}
