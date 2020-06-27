package org.apache.spark.search.rdd

import java.io.{IOException, ObjectOutputStream}

import org.apache.lucene.search.Query
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.search.{SearchException, SearchRecord}
import org.apache.spark.util.Utils

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

  override val partitioner: Option[Partitioner] = searchRDD.partitioner

  override protected def getPreferredLocations(split: Partition): Seq[String] =
    firstParent[H].asInstanceOf[SearchRDD[H]]
      .getPreferredLocations(split.asInstanceOf[MatchRDDPartition].searchPartition)

  override def compute(split: Partition, context: TaskContext): Iterator[(Long, Iterator[SearchRecord[H]])] = {
    val matchPartition = split.asInstanceOf[MatchRDDPartition]

    // Be sure parent partition is indexed
    parent[H](0).iterator(matchPartition.searchPartition, context)

    // Match other partitions against our
    tryAndClose(parent[H](0).asInstanceOf[SearchRDD[H]].reader(matchPartition.searchPartition.index,
      matchPartition.searchPartition.searchIndexPartition.indexDir)) {
      spr => matchPartition.otherPartitions.flatMap(op =>
        parent[(Long, S)](1).iterator(op, context)
          .map(docIndex => (docIndex._1,
            try {
              spr.search(queryBuilder(docIndex._2), topK, minScore).map(searchRecordJavaToProduct).toSeq.iterator
            } catch {
              case e: SearchException => throw new SearchException(s"error during matching ${docIndex._1}: ${docIndex._2}", e)
            })
          )).toIterator
    }
  }

  override def clearDependencies() {
    super.clearDependencies()
    searchRDD = null
    other = null
  }

  override protected def getPartitions: Array[Partition] = {
    val parts = new Array[Partition](searchRDD.partitions.length * other.partitions.length)
    for (s1 <- parent[H](0).partitions) {
      parts(s1.index) = new MatchRDDPartition(s1.index, searchRDD, other)
    }
    parts
  }

  override def getDependencies: Seq[Dependency[_]] = Seq(
    new OneToOneDependency(searchRDD)
  )

  class MatchRDDPartition(val idx: Int,
                          @transient private val searchRDD: SearchRDD[H],
                          @transient private val other: RDD[(Long, S)]) extends Partition {
    override def index: Int = idx

    var searchPartition: SearchPartition[H] = searchRDD.partitions(idx).asInstanceOf[SearchPartition[H]]
    var otherPartitions: Array[Partition] = other.partitions

    @throws(classOf[IOException])
    private def writeObject(oos: ObjectOutputStream): Unit = Utils.tryOrIOException {
      // Update the reference to parent split at the time of task serialization
      searchPartition = searchRDD.partitions(idx).asInstanceOf[SearchPartition[H]]
      otherPartitions = other.partitions
      oos.defaultWriteObject()
    }
  }

}
