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

import java.util.{List => JList}
import java.{lang => jl}

import org.apache.spark.api.java.JavaRDD

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

/**
 * Java friendly version of [[SearchRDD]].
 */
class SearchJavaBaseRDD[T: ClassTag](rdd: JavaRDD[T], opts: SearchRDDOptions[T])
  extends JavaRDD[T](rdd.rdd) with ISearchRDDJava[T] {

  protected val searchRDD: SearchRDD[T] = rdd.rdd.searchRDD(opts)

  /**
   * [[org.apache.spark.search.rdd.SearchRDD#count(java.lang.String)]]
   */
  override def count(query: String): jl.Long =
    searchRDD.count(query)

  /**
   * [[org.apache.spark.search.rdd.SearchRDD#searchList(java.lang.String, int)]]
   */
  override def searchList(query: String, topK: jl.Integer): JList[SearchRecord[T]] =
    searchRDD.searchList(query, topK).asJava
}