package org.apache.spark.search.rdd

import org.apache.lucene.search.Query
import org.apache.spark.rdd.RDD
import org.apache.spark.search.SearchRecord
import org.apache.spark.{OneToOneDependency, Partition, TaskContext}

import scala.reflect.ClassTag

/**
 * Result RDD of a [[org.apache.spark.search.rdd.SearchRDD#searchQueryJoin(org.apache.spark.rdd.RDD, scala.Function1, int, double)]]
 *
 * @author Pierrick HYMBERT
 */
private[rdd] class MatchRDD[S: ClassTag, H: ClassTag](val searchRDD: SearchRDD[H],
                                                      val other: RDD[(Long, S)],
                                                      queryBuilder: S => Query,
                                                      topK: Int = Int.MaxValue,
                                                      minScore: Double = 0)
  extends RDD[(Long, Iterator[SearchRecord[H]])](searchRDD.context, Seq(new OneToOneDependency(searchRDD))) {

  override def compute(split: Partition, context: TaskContext): Iterator[(Long, Iterator[SearchRecord[H]])] = {
    val matchPartition = split.asInstanceOf[MatchRDDPartition]

    // compute our current search rdd partition if needed
    searchRDD.iterator(matchPartition.searchPartition, context)

    // Match other partitions against our
    tryAndClose(searchRDD.reader(matchPartition.index, matchPartition.searchPartition.indexDir)) {
      spr =>
        matchPartition.otherPartitions.flatMap(otherPart => {
          other.iterator(otherPart, context)
            .map(docIndex => (docIndex._1,
              spr.search(queryBuilder(docIndex._2), topK, minScore).map(searchRecordJavaToProduct).toSeq.iterator)
            )
        }).iterator
    }
  }

  override protected def getPreferredLocations(split: Partition): Seq[String] =
    searchRDD.getPreferredLocations(split)

  override protected def getPartitions: Array[Partition] = {
    searchRDD.partitions.map(searchPartition => new MatchRDDPartition(searchPartition.asInstanceOf[SearchPartition[H]], other.partitions))
  }

  private class MatchRDDPartition(val searchPartition: SearchPartition[H], val otherPartitions: Array[Partition]) extends Partition {
    override def index: Int = searchPartition.index
  }

}
