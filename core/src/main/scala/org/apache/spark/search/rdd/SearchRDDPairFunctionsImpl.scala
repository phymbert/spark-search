package org.apache.spark.search.rdd

import org.apache.lucene.search.Query
import org.apache.spark.rdd.RDD
import org.apache.spark.search.{SearchOptions, SearchRecord, defaultOpts, defaultQueryBuilder, queryStringBuilder}
import org.apache.spark.util.BoundedPriorityQueue

import scala.reflect.ClassTag

/**
 * Add pair functions to search RDD.
 *
 * @author Pierrick HYMBERT
 */
class SearchRDDPairFunctionsImpl[K: ClassTag, S: ClassTag](val pairRdd: RDD[(K, S)],
                                                           val valueSearchOptions: SearchOptions[S] = defaultOpts[S])
  extends RDDWithSearch[(K, S)](pairRdd, valueSearchOptions.asInstanceOf[SearchOptions[(K, S)]])
    with Serializable {

  /**
   * Searches join for this input RDD elements matches against these ones
   * by building a lucene query string per doc
   * and returns matching hit as tuples.
   *
   * @param other        to match with
   * @param queryBuilder builds the query string to match with the searched document
   * @param topK         – topK to return by partition
   * @param minScore     minimum score of matching documents
   * @tparam W Doc type to match with
   * @return matches doc and related hit RDD
   */
  def searchJoin[W](other: RDD[W],
                    queryBuilder: W => String,
                    topK: Int = Int.MaxValue,
                    minScore: Double = 0)
                   (implicit wClassTag: ClassTag[W]): RDD[(K, (W, SearchRecord[(K, S)]))] =
    searchJoinQuery(other, queryStringBuilder(queryBuilder, options), topK, minScore)

  /**
   * Searches join for this input RDD elements matches against these ones
   * by building a lucene query per doc
   * and returns matching hit as tuples.
   *
   * @param other        to match with
   * @param queryBuilder builds the query string to match with the searched document
   * @param topK         – topK to return by partition
   * @param minScore     minimum score of matching documents
   * @tparam W Doc type to match with
   * @return matches doc and related hit RDD
   */
  def searchJoinQuery[W](other: RDD[W],
                         queryBuilder: W => Query,
                         topK: Int = Int.MaxValue,
                         minScore: Double = 0)
                        (implicit wClassTag: ClassTag[W]): RDD[(K, (W, SearchRecord[(K, S)]))] = {
    val cartesianRDD: RDD[(W, SearchRecord[(K, S)])] = new SearchRDDCartesian[W, (K, S)](
      _searchRDD.asInstanceOf[SearchRDDLucene[(K, S)]].indexerRDD,
      other, queryBuilder,
      options.getReaderOptions, topK, minScore
    )

    val pairedRDD: RDD[(K, (W, SearchRecord[(K, S)]))] = cartesianRDD.map({
      case ((k: K, w: W), sr: SearchRecord[(K, S)]) => (k, (w, sr))
    })

    // TopK monoid
    // FIXME avoid aggregation by key and do it in one pass with a fold
    val ord: Ordering[(W, SearchRecord[(K, S)])] = Ordering.by(_._2.score)
    val topKMonoid: RDD[(K, (W, SearchRecord[(K, S)]))] = pairedRDD
      .aggregateByKey(new BoundedPriorityQueue[(W, SearchRecord[(K, S)])](topK)(ord.reverse))(
        seqOp = (topK, searchRecord) => topK += searchRecord,
        combOp = (topK1, topK2) => topK1 ++= topK2
      ).flatMapValues(_.toList)
    topKMonoid
  }

  /**
   * Alias for
   * [[org.apache.spark.search.rdd.SearchRDD#searchDropDuplicates(scala.Function1, int, double, int)}]]
   */
  def distinct(numPartitions: Int): RDD[(K, S)] =
    searchDropDuplicates()

  /**
   * Drops duplicated records by applying lookup for matching hits of the query against this RDD.
   *
   * @param queryBuilder builds the lucene query to search for duplicate
   * @param minScore     minimum score of matching documents
   */
  def searchDropDuplicates[C](queryBuilder: S => Query = defaultQueryBuilder(opts),
                              minScore: Double = 0,
                              createCombiner: S => C = identity(_: S).asInstanceOf[C],
                              mergeValue: (C, S) => C = (_: C, s: S) => s.asInstanceOf[C],
                              mergeCombiners: (C, C) => C = (c: C, _: C) => c
                             ): RDD[(K, C)] = {
    val unwrapDoc = rdd.sparkContext.clean((kv: (K, S)) => queryBuilder(kv._2))

    val cartesianRDD: RDD[((K, S), SearchRecord[(K, S)])] =
      new SearchRDDCartesian[(K, S), (K, S)](
        _searchRDD.asInstanceOf[SearchRDDLucene[(K, S)]].indexerRDD, rdd,
        unwrapDoc,
        options.getReaderOptions,
        Integer.MAX_VALUE,
        minScore
      )

    val pairedRDD: RDD[(K, S)] = cartesianRDD.flatMap(context.clean {
      case ((k: K, s: S), sr: SearchRecord[(K, S)]) =>
        Iterator((k, s), (sr.source._1, sr.source._2))
    })

    pairedRDD.combineByKey(createCombiner, mergeValue, mergeCombiners)
  }

  private def context = pairRdd.sparkContext
}
