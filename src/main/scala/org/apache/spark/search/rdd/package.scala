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

import java.util.function.{Function => JFunction}

import org.apache.lucene.search.Query
import org.apache.spark.rdd.RDD

import scala.language.implicitConversions
import scala.reflect.ClassTag

/**
 * Spark Search RDD.
 */
package object rdd {

  /**
   * Search record.
   */
  case class SearchRecord[T](id: Long, partitionIndex: Long, score: Double, shardIndex: Long, source: T)

  /**
   * Matched record.
   */
  case class Match[S, H](doc: S, hits: List[SearchRecord[H]])

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

  implicit def indexOptions[T](optionsBuilderFunc: Function[IndexationOptions.Builder[T], IndexationOptions.Builder[T]]): JFunction[IndexationOptions.Builder[T], IndexationOptions.Builder[T]] =
    new JFunction[IndexationOptions.Builder[T], IndexationOptions.Builder[T]] {
      override def apply(opts: IndexationOptions.Builder[T]): IndexationOptions.Builder[T] = {
        optionsBuilderFunc.apply(opts)
      }
    }

  implicit def readerOptions[T](optionsBuilderFunc: Function[ReaderOptions.Builder[T], ReaderOptions.Builder[T]]): JFunction[ReaderOptions.Builder[T], ReaderOptions.Builder[T]] =
    new JFunction[ReaderOptions.Builder[T], ReaderOptions.Builder[T]] {
      override def apply(opts: ReaderOptions.Builder[T]): ReaderOptions.Builder[T] = {
        optionsBuilderFunc.apply(opts)
      }
    }

  private[rdd] def searchRecordJavaToProduct[T](sr: SearchRecordJava[T]) = {
    SearchRecord(sr.id, sr.partitionIndex, sr.score, sr.shardIndex, sr.source)
  }
}
