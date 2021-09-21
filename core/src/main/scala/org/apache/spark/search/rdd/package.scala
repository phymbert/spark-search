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
package org.apache.spark.search

import org.apache.lucene.queryparser.classic.QueryParser
import org.apache.lucene.search.Query
import org.apache.spark.rdd.RDD
import org.apache.spark.{ExecutorAllocationClient, SparkContext}

import scala.language.implicitConversions
import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

/**
 * Spark Search RDD.
 */
package object rdd {

  implicit def rddWithSearch[S: ClassTag](rdd: RDD[S]): RDDWithSearch[S] =
    new RDDWithSearch[S](rdd)

  implicit def pairRddWithSearch[K: ClassTag, V: ClassTag](rdd: RDD[(K, V)]): SearchRDDPairFunctions[K, V] =
    new SearchRDDPairFunctions[K, V](rdd)

  implicit def searchAsRDD[S: ClassTag](searchRDD: SearchRDD[S]): RDD[S] =
    searchRDD.asInstanceOf[RDD[S]]

  implicit def searchAsSearchPairRDD[K: ClassTag, V: ClassTag](searchRDD: SearchRDD[(K, V)]): SearchRDDPairFunctions[K, V] =
    new SearchRDDPairFunctions[K, V](searchRDD, searchRDD.options)

  /**
   * Provide a static query to pass to SearchRDD serializable.
   *
   * /!\ Important as Lucene Query is not serializable.
   */
  type StaticQueryProvider = () => Query

  def parseQueryString[S](queryString: String, opts: SearchOptions[_] = defaultOpts): StaticQueryProvider =
  // Query parser is not thread safe
    () => new QueryParser(opts.getReaderOptions.getDefaultFieldName, opts.getReaderOptions.analyzer.newInstance())
      .parse(queryString)


  private[rdd] def searchRecordJavaToProduct[S](sr: SearchRecordJava[S]) = {
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

  private[rdd] def getPreferredLocation(sc: SparkContext,
                                        index: Int,
                                        numPartition: Int,
                                        parentPreferredLocation: Seq[String]): Array[String] =
    sc.schedulerBackend match {
      case b: ExecutorAllocationClient =>
        // Try to balance partitions across executors
        val allIds = sc.getExecutorIds()
        val ids = allIds.grouped(numPartition).toList
        ids(index % ids.length).toArray
      case _ => parentPreferredLocation.toArray
    }
}
