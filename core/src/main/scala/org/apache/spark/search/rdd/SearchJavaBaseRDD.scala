/**
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
package org.apache.spark.search.rdd

import org.apache.spark.api.java.JavaRDD
import org.apache.spark.search._
import org.apache.spark.search.rdd.ISearchRDDJava.{QueryBuilder, QueryStringBuilder}

import scala.reflect.ClassTag

/**
 * Java friendly version of [[SearchRDD]].
 */
class SearchJavaBaseRDD[T: ClassTag](rdd: JavaRDD[T], opts: SearchOptions[T])
  extends JavaRDD[T](rdd.rdd) with ISearchRDDJava[T] {

  protected val searchRDD: SearchRDD[T] = rdd.rdd.searchRDD(opts)

  override def count(query: String): Long =
    searchRDD.count(parseQueryString(query))

  override def searchList(query: String, topK: Int): Array[SearchRecordJava[T]] =
    searchRDD.searchListQuery(parseQueryString(query), topK).map(searchRecordAsJava(_))

  override def searchList(query: String, topK: Int, minScore: Double): Array[SearchRecordJava[T]] =
    searchRDD.searchListQuery(parseQueryString(query), topK, minScore).map(searchRecordAsJava)

  override def search(query: String, topK: Int, minScore: Double): JavaRDD[SearchRecordJava[T]] =
    searchRDD.search(query, topK).map(searchRecordAsJava(_)).toJavaRDD()

  override def searchJoin[S](rdd: JavaRDD[S],
                             queryBuilder: QueryStringBuilder[S],
                             topK: Int,
                             minScore: Double): JavaRDD[MatchJava[S, T]] = {
    implicit val classTag: ClassTag[S] = rdd.classTag
    searchRDD.searchJoin(rdd.rdd, (s:S) => queryBuilder.build(s), topK, minScore)
    .map(matchAsJava(_))
  }

  override def searchJoinQuery[S](rdd: JavaRDD[S],
                                  queryBuilder: QueryBuilder[S],
                                  topK: Int,
                                  minScore: Double): JavaRDD[MatchJava[S, T]] = {
    implicit val classTag: ClassTag[S] = rdd.classTag
    searchRDD.searchJoinQuery(rdd.rdd, (s:S) => queryBuilder.build(s), topK, minScore)
      .map(matchAsJava(_))
  }


  private def matchAsJava[S: ClassTag](s: Match[S, T]): MatchJava[S, T] = {
    new MatchJava[S, T](s.doc, s.hits.map(searchRecordAsJava))
  }

  private def searchRecordAsJava(sr: SearchRecord[T]) =
    new SearchRecordJava[T](sr.id, sr.partitionIndex, sr.score, sr.shardIndex, sr.source)
}