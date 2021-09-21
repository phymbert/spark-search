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
package org.apache.spark

import java.util.function.{Function => JFunction}

import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.queryparser.classic.QueryParser
import org.apache.lucene.search.Query
import org.apache.lucene.util.QueryBuilder
import org.apache.spark.search.reflect.DefaultQueryBuilder

import scala.collection.Iterable
import scala.language.implicitConversions
import scala.reflect.ClassTag

/**
 * Spark Search brings advanced full text search features
 * to your Dataframe, Dataset and RDD. Powered by Apache Lucene.
 */
package object search {

  /**
   * Search record.
   */
  case class SearchRecord[S](id: Long, partitionIndex: Long, score: Double, shardIndex: Long, source: S)
    extends Ordering[SearchRecord[S]] {
    override def compare(sr1: SearchRecord[S], sr2: SearchRecord[S]): Int = sr2.score.compare(sr1.score) // Reverse
  }

  /**
   * Default search options.
   */
  def defaultOpts[S]: SearchOptions[S] = SearchOptions.defaultOptions.asInstanceOf[SearchOptions[S]]

  /**
   * Abstract class to ease building lucene queries using query string lucene syntax with spark search RDD.
   *
   * @param queryStringBuilder Generate lucene query string for this input element
   * @tparam S Type of input class
   */
  class QueryStringBuilderWithAnalyzer[S](val queryStringBuilder: S => String,
                                          val defaultFieldName: String = ReaderOptions.DEFAULT_FIELD_NAME,
                                          override val analyzerClass: Class[_ <: Analyzer] = classOf[StandardAnalyzer])
    extends CanBuildQueryWithAnalyzer[S](analyzerClass) {

    override def apply(s: S): Query =
      new QueryParser(defaultFieldName, _analyzer).parse(queryStringBuilder.apply(s))
  }

  /**
   * Abstract class to ease building lucene query with spark search RDD, support serialization
   * and query builder creation in a distributed world.
   *
   * @param queryBuilder Generate lucene query for this input element
   * @tparam S Type of input class
   */
  class QueryBuilderWithAnalyzer[S](queryBuilder: (S, QueryBuilder) => Query,
                                    override val analyzerClass: Class[_ <: Analyzer] = classOf[StandardAnalyzer]
                                   )
    extends CanBuildQueryWithAnalyzer[S](analyzerClass) {

    @transient private lazy val _luceneQueryBuilder: QueryBuilder = new QueryBuilder(_analyzer)

    override def apply(s: S): Query = queryBuilder.apply(s, _luceneQueryBuilder)
  }

  /**
   * Abstract class to ease building lucene query with spark search RDD, support serialization
   * and analyzer creation in a distributed world.
   *
   * @param analyzerClass Type of the analyzer to use with the query
   * @tparam S Type of input class
   */
  abstract class CanBuildQueryWithAnalyzer[S](val analyzerClass: Class[_ <: Analyzer] = classOf[StandardAnalyzer])
    extends (S => Query) with Serializable {

    @transient lazy val _analyzer: Analyzer = analyzerClass.newInstance()
  }

  def defaultQueryBuilder[S: ClassTag](opts: SearchOptions[_] = defaultOpts)(implicit cls: ClassTag[S]): S => Query =
    new QueryBuilderWithAnalyzer[S](new DefaultQueryBuilder[S](cls.runtimeClass.asInstanceOf[Class[_ <: S]]).asInstanceOf[(S, QueryBuilder) => Query],
      opts.getReaderOptions.analyzer)

  def queryBuilder[S](builder: (S, QueryBuilder) => Query, opts: SearchOptions[_] = defaultOpts): S => Query =
    new QueryBuilderWithAnalyzer[S](builder, opts.getReaderOptions.analyzer)

  def queryStringBuilder[S](builder: S => String, opts: SearchOptions[_] = defaultOpts): S => Query =
    new QueryStringBuilderWithAnalyzer[S](builder, opts.getReaderOptions.getDefaultFieldName, opts.getReaderOptions.analyzer)

  implicit def indexOptions[S](optionsBuilderFunc: Function[IndexationOptions.Builder[S], IndexationOptions.Builder[S]]): JFunction[IndexationOptions.Builder[S], IndexationOptions.Builder[S]] =
    new JFunction[IndexationOptions.Builder[S], IndexationOptions.Builder[S]] {
      override def apply(opts: IndexationOptions.Builder[S]): IndexationOptions.Builder[S] = {
        optionsBuilderFunc.apply(opts)
      }
    }

  implicit def readerOptions[S](optionsBuilderFunc: Function[ReaderOptions.Builder[S], ReaderOptions.Builder[S]]): JFunction[ReaderOptions.Builder[S], ReaderOptions.Builder[S]] =
    new JFunction[ReaderOptions.Builder[S], ReaderOptions.Builder[S]] {
      override def apply(opts: ReaderOptions.Builder[S]): ReaderOptions.Builder[S] = {
        optionsBuilderFunc.apply(opts)
      }
    }

  private[search] def searchRecordJavaToProduct[S](sr: SearchRecordJava[S]) = {
    SearchRecord(sr.id, sr.partitionIndex, sr.score, sr.shardIndex, sr.source)
  }
}
