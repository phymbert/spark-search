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

import org.apache.lucene.search.Query
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.search.rdd.ISearchRDDJava.{QueryBuilder, QueryStringBuilder}

import scala.reflect.ClassTag

/**
 * Java friendly version of [[SearchRDD]].
 */
abstract class SearchJavaBaseRDD[T: ClassTag](rdd: JavaRDD[T], opts: SearchRDDOptions[T])
  extends JavaRDD[T](rdd.rdd) with ISearchRDDJava[T] {

  protected val searchRDD: SearchRDD[T] = rdd.rdd.searchRDD(opts)

  override def count(query: String): Long =
    searchRDD.count(query)

  override def searchList(query: String, topK: Int, minScore: Float): Array[SearchRecord[T]] =
    searchRDD.searchList(query, topK, minScore)

  override def searchList(query: Query, topK: Int, minScore: Float): Array[SearchRecord[T]] =
    searchRDD.searchList(query, topK, minScore)

  override def search(query: String, topK: Int, minScore: Float): JavaRDD[SearchRecord[T]] =
    searchRDD.search(query, topK).toJavaRDD()

  override def searchJoin[S](rdd: JavaRDD[S], queryStringBuilder: QueryStringBuilder[S], topK: Int, minScore: Float): JavaRDD[Match[S, T]] =
    searchRDD.searchJoin(rdd, (s:S) => queryStringBuilder.build(s), topK, minScore).toJavaRDD()

  override def searchJoin[S](rdd: JavaRDD[S], queryBuilder: QueryBuilder[S], topK: Int, minScore: Float): JavaRDD[Match[S, T]] =
    searchRDD.searchJoin(rdd, (s:S) => queryBuilder.build(s), topK, minScore).toJavaRDD()

}