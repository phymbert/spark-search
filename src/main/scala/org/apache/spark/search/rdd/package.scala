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
import org.apache.spark.rdd.RDD

import scala.language.implicitConversions
import scala.reflect.ClassTag

/**
 * Spark Search RDD.
 */
package object rdd {

  /**
   * Default search options.
   */
  def defaultOpts[T]: SearchRDDOptions[T] = SearchRDDOptions.defaultOptions.asInstanceOf[SearchRDDOptions[T]]

  /**
   * A search query to run against a [[org.apache.spark.search.rdd.SearchRDD]],
   * can be a string to be parsed by [[org.apache.lucene.queryparser.classic.QueryParser]]
   * or directly a [[org.apache.lucene.search.Query]].
   */
  trait SearchQuery

  case class SearchQueryString(queryString: String) extends SearchQuery

  case class SearchLuceneQuery(query: Query) extends SearchQuery

  implicit def luceneQuery(query: Query): SearchQuery = SearchLuceneQuery(query)

  implicit def queryString(queryString: String): SearchQuery = SearchQueryString(queryString)

  implicit def rddWithSearch[T: ClassTag](rdd: RDD[T]): RDDWithSearch[T] = new RDDWithSearch[T](rdd)
}
