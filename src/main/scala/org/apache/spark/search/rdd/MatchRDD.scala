package org.apache.spark.search.rdd

import org.apache.lucene.search.Query
import org.apache.spark.rdd.RDD
import org.apache.spark.{OneToOneDependency, Partition, TaskContext}

import scala.reflect.ClassTag

/**
 * Result RDD of a [[org.apache.spark.search.rdd.SearchRDD#searchQueryJoin(org.apache.spark.rdd.RDD, scala.Function1, int, double)]]
 *
 * @author Pierrick HYMBERT
 */
private[rdd] class MatchRDD[S: ClassTag, H: ClassTag](val searchRDD: SearchRDD[H],
                                                      val other: RDD[S],
                                                      queryBuilder: S => Query,
                                                      topK: Int = Int.MaxValue,
                                                      minScore: Double = 0)
  extends RDD[(Long, Iterator[SearchRecord[H]])](searchRDD.context, Seq(new OneToOneDependency(searchRDD))) {

  override def compute(split: Partition, context: TaskContext): Iterator[(Long, Iterator[SearchRecord[H]])] = {
    val searchPartition = split.asInstanceOf[SearchPartition[H]]

    // compute our search rdd partition if needed
    searchRDD.iterator(searchPartition, context)

    val otherNumPartition = other.getNumPartitions

    // Match other partitions against our
    tryAndClose(searchRDD.reader(searchPartition.index, searchPartition.indexDir)) {
      spr =>
        (0 until otherNumPartition).flatMap(partIndex => {
          other.iterator(other.partitions(partIndex), context).zipWithIndex.map(_.swap)
            .map(docIndex => (partIndex.toLong * otherNumPartition + docIndex._1,
              spr.search(queryBuilder(docIndex._2), topK, minScore).map(searchRecordJavaToProduct).toSeq.iterator)
            )
        }).iterator
    }
  }

  override protected def getPreferredLocations(split: Partition): Seq[String] =
    searchRDD.getPreferredLocations(split)

  override protected def getPartitions: Array[Partition] = searchRDD.partitions
}
