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

    // compute search rdd
    searchRDD.iterator(searchPartition, context)

    val otherPartitions = other.partitions
    val otherWithPartitionIndex = otherPartitions.zipWithIndex.map(_.swap)

    tryAndClose(searchRDD.reader(searchPartition.index, searchPartition.indexDir)) {
      spr => otherWithPartitionIndex.flatMap(p =>
          other.iterator(p._2, context).zipWithIndex.map(_.swap)
            .map(docIndex => (p._1.toLong * otherPartitions.length + docIndex._1,
              spr.search(queryBuilder(docIndex._2), topK, minScore).map(searchRecordJavaToProduct).toSeq.iterator)
            )).iterator
    }
  }

  override protected def getPreferredLocations(split: Partition): Seq[String] =
    searchRDD.getPreferredLocations(split)

  override protected def getPartitions: Array[Partition] = searchRDD.partitions
}
