package org.apache.spark.search.rdd

import java.io.{IOException, ObjectOutputStream}

import org.apache.lucene.search.Query
import org.apache.spark.rdd.RDD
import org.apache.spark.search.{SearchException, SearchRecord}
import org.apache.spark.util.Utils
import org.apache.spark.{Dependency, NarrowDependency, OneToOneDependency, Partition, RangeDependency, TaskContext}

import scala.reflect.ClassTag

/**
 * Result RDD of a [[org.apache.spark.search.rdd.SearchRDD#searchQueryJoin(org.apache.spark.rdd.RDD, scala.Function1, int, double)]]
 *
 * @author Pierrick HYMBERT
 */
class MatchRDD[S: ClassTag, H: ClassTag](@transient var searchRDD: SearchRDD[H],
                                         @transient var other: RDD[(Long, S)],
                                         queryBuilder: S => Query,
                                         topK: Int = Int.MaxValue,
                                         minScore: Double = 0)
  extends RDD[(Long, Iterator[SearchRecord[H]])](searchRDD.context, Nil)
    with Serializable {

  override def compute(split: Partition, context: TaskContext): Iterator[(Long, Iterator[SearchRecord[H]])] = {
    val matchPartition = split.asInstanceOf[MatchRDDPartition]

    // Be sure search partition is indexed
    if (matchPartition.otherPartitionIndex == 0)
      parent[H](0).iterator(matchPartition.searchPartition, context)

    // Match other partitions against our
    tryAndClose(parent[H](0).asInstanceOf[SearchRDD[H]].reader(matchPartition.searchPartition.index, matchPartition.searchPartition.indexDir)) {
      spr =>
        parent[(Long, S)](1).iterator(matchPartition.otherPartition, context)
          .map(docIndex => (docIndex._1,
            try {
              spr.search(queryBuilder(docIndex._2), topK, minScore).map(searchRecordJavaToProduct).toSeq.iterator
            } catch {
              case e: SearchException => throw new SearchException(s"error during matching ${docIndex._1}: ${docIndex._2}", e)
            })
          ).toArray.toIterator
    }
  }

  override def clearDependencies() {
    super.clearDependencies()
    searchRDD = null
    other = null
  }

  private val numPartitionsInOther = other.partitions.length

  override protected def getPreferredLocations(split: Partition): Seq[String] =
    parent[H](0).asInstanceOf[SearchRDD[H]]
      .getPreferredLocations(split.asInstanceOf[MatchRDDPartition].searchPartition)

  override protected def getPartitions: Array[Partition] = {
    val parts = new Array[Partition](searchRDD.partitions.length * other.partitions.length)
    for (s2 <- parent[(Long, S)](1).partitions; s1 <- parent[H](0).partitions) {
      val idx = s1.index * numPartitionsInOther + s2.index
      parts(idx) = new MatchRDDPartition(idx, s1.index, s2.index, searchRDD, other)
    }
    parts
  }

  override def getDependencies: Seq[Dependency[_]] = List(
    new OneToOneDependency(searchRDD),
    new OneToOneDependency(other)
  )

  class MatchRDDPartition(val idx: Int,
                          val searchPartitionIndex: Int,
                          val otherPartitionIndex: Int,
                          @transient private val searchRDD: SearchRDD[H],
                          @transient private val other: RDD[(Long, S)]) extends Partition {
    override def index: Int = idx

    var searchPartition: SearchPartition[H] = searchRDD.partitions(searchPartitionIndex).asInstanceOf[SearchPartition[H]]
    var otherPartition: Partition = other.partitions(otherPartitionIndex)

    @throws(classOf[IOException])
    private def writeObject(oos: ObjectOutputStream): Unit = Utils.tryOrIOException {
      // Update the reference to parent split at the time of task serialization
      searchPartition = searchRDD.partitions(searchPartitionIndex).asInstanceOf[SearchPartition[H]]
      otherPartition = other.partitions(otherPartitionIndex)
      oos.defaultWriteObject()
    }
  }

}
