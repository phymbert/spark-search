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

import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.queryparser.classic.QueryParser
import org.apache.lucene.search.Query
import org.apache.lucene.util.QueryBuilder
import org.apache.spark.rdd.RDD

import scala.language.implicitConversions
import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

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
  case class Match[S, H](doc: S, hits: Array[SearchRecord[H]])

  /**
   * Default search options.
   */
  def defaultOpts[T]: SearchRDDOptions[T] = SearchRDDOptions.defaultOptions.asInstanceOf[SearchRDDOptions[T]]

  /**
   * Abstract class to ease building lucene queries using query string lucene syntax with spark search RDD.
   *
   * @param queryStringBuilder Generate lucene query string for this input element
   * @tparam T Type of input class
   */
  class QueryStringBuilderWithAnalyzer[T](val queryStringBuilder: T => String,
                                          val defaultFieldName: String = ReaderOptions.DEFAULT_FIELD_NAME)
    extends CanBuildQueryWithAnalyzer[T] {

    override def apply(t: T): Query =
      new QueryParser(defaultFieldName, _analyzer).parse(queryStringBuilder.apply(t))
  }

  /**
   * Abstract class to ease building lucene query with spark search RDD, support serialization
   * and query builder creation in a distributed world.
   *
   * @param queryBuilder Generate lucene query for this input element
   * @tparam T Type of input class
   */
  class QueryBuilderWithAnalyzer[T](val queryBuilder: (T, QueryBuilder) => Query) extends CanBuildQueryWithAnalyzer[T] {

    @transient private lazy val _luceneQueryBuilder: QueryBuilder = new QueryBuilder(_analyzer)

    override def apply(t: T): Query = queryBuilder.apply(t, _luceneQueryBuilder)
  }

  /**
   * Abstract class to ease building lucene query with spark search RDD, support serialization
   * and analyzer creation in a distributed world.
   *
   * @param analyzerClass Type of the analyzer to use with the query
   * @tparam T Type of input class
   */
  abstract class CanBuildQueryWithAnalyzer[T](val analyzerClass: Class[_ <: Analyzer] = classOf[StandardAnalyzer])
    extends (T => Query) with Serializable {

    @transient lazy val _analyzer: Analyzer = analyzerClass.newInstance()
  }

  def queryStringBuilder[T](builder: T => String, opts: SearchRDDOptions[_] = defaultOpts): T => Query =
    new QueryStringBuilderWithAnalyzer[T](builder, opts.getReaderOptions.getDefaultFieldName)

  def parseQueryString[T](queryString: String, opts: SearchRDDOptions[_] = defaultOpts): () => Query =
  // Query parser is not thread safe
    () => new QueryParser(opts.getReaderOptions.getDefaultFieldName, opts.getReaderOptions.analyzer.newInstance())
      .parse(queryString)

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

  private[rdd] def tryAndClose[A <: AutoCloseable, B](resource: A)(block: A => B): B = {
    Try(block(resource)) match {
      case Success(result) =>
        resource.close()
        result
      case Failure(e) =>
        resource.close()
        throw e
    }
  }
}
