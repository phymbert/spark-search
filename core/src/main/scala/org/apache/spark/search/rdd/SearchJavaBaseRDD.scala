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
package org.apache.spark.search.rdd

import org.apache.spark.SparkContext
import org.apache.spark.api.java.{JavaPairRDD, JavaRDD}
import org.apache.spark.search._
import org.apache.spark.search.rdd.SearchRDDJava.{QueryBuilder, QueryStringBuilder}

import scala.reflect.ClassTag

/**
 * Java friendly version of [[SearchRDD]].
 */
trait SearchRDDJavaWrapper[S] extends SearchRDDJava[S] {

  val searchRDD: SearchRDD[S]

  val classTag: ClassTag[S]

  override def count(): Long = searchRDD.count()

  override def count(query: String): Long =
    searchRDD.count(parseQueryString(query))

  override def searchList(query: String, topK: Int): Array[SearchRecordJava[S]] =
    searchRDD.searchListQuery(parseQueryString(query), topK).map(searchRecordAsJava(_))

  override def searchList(query: String, topK: Int, minScore: Double): Array[SearchRecordJava[S]] =
    searchRDD.searchListQuery(parseQueryString(query), topK, minScore).map(searchRecordAsJava)

  override def search(query: String, topK: Int, minScore: Double): JavaRDD[SearchRecordJava[S]] =
    searchRDD.search(query, topK).map(searchRecordAsJava(_)).toJavaRDD()

  override def matches[K, V](rdd: JavaPairRDD[K, V],
                             queryBuilder: QueryStringBuilder[V],
                             topK: Int,
                             minScore: Double): JavaPairRDD[K, (V, Array[SearchRecordJava[S]])] = {
    implicit val kClassTag: ClassTag[K] = rdd.kClassTag
    implicit val vClassTag: ClassTag[V] = rdd.vClassTag
    new JavaPairRDD(
      searchRDD.matches[K, V](rdd.rdd, v => queryBuilder.build(v), topK, minScore)
        .mapValues(m => (m._1, m._2.map(searchRecordAsJava(_)))))
  }

  override def save(path: String): Unit = searchRDD.save(path)

  override def matchesQuery[K, V](rdd: JavaPairRDD[K, V],
                                  queryBuilder: QueryBuilder[V],
                                  topK: Int,
                                  minScore: Double): JavaPairRDD[K, (V, Array[SearchRecordJava[S]])] = {
    implicit val kClassTag: ClassTag[K] = rdd.kClassTag
    implicit val vClassTag: ClassTag[V] = rdd.vClassTag
    new JavaPairRDD(
      searchRDD.matchesQuery[K, V](rdd.rdd, v => queryBuilder.build(v), topK, minScore)
        .mapValues(m => (m._1, m._2.map(searchRecordAsJava(_)))))
  }

  override def javaRDD(): JavaRDD[S] = {
    implicit val classTag: ClassTag[S] = this.classTag
    new JavaRDD(searchAsRDD(searchRDD))
  }

  private def searchRecordAsJava(sr: SearchRecord[S]) =
    new SearchRecordJava[S](sr.id, sr.partitionIndex, sr.score, sr.shardIndex, sr.source)
}

class SearchJavaBaseRDD[T: ClassTag](rdd: JavaRDD[T], opts: SearchOptions[T])
  extends JavaRDD[T](rdd.rdd)
    with SearchRDDJavaWrapper[T] {
  override lazy val searchRDD: SearchRDD[T] = rdd.rdd.searchRDD(opts)
  override val classTag: ClassTag[T] = searchRDD.elementClassTag

  override def count(): Long = searchRDD.count()
}

class SearchRDDReloadedJava[T: ClassTag](val searchRdd: SearchRDD[T])
  extends SearchRDDJavaWrapper[T]
    with Serializable {
  override lazy val searchRDD: SearchRDD[T] = searchRdd
  override val classTag: ClassTag[T] = searchRDD.elementClassTag
}

object SearchRDDReloadedJava {
  def load[T: ClassTag](sc: SparkContext,
                        path: String,
                        options: SearchOptions[T]
                       ): SearchRDDJava[T] = new SearchRDDReloadedJava(SearchRDD.load(sc, path, options))
}