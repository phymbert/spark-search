package org.apache.spark.search.rdd

import java.io.{IOException, ObjectOutputStream}
import java.nio.file.Paths

import org.apache.lucene.index.DirectoryReader
import org.apache.lucene.search.{IndexSearcher, MatchAllDocsQuery, Query}
import org.apache.spark.rdd.RDD
import org.apache.spark.search.{SearchException, SearchRecord}
import org.apache.spark.util.Utils
import org.apache.spark.{Dependency, NarrowDependency, Partition, TaskContext}

import scala.reflect.ClassTag
import scala.util.control.Breaks._

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

  var options = searchRDD.options

  @throws(classOf[IOException])
  private def writeObject(oos: ObjectOutputStream): Unit = Utils.tryOrIOException {
    // Update the reference to parent split at the time of task serialization
    options = searchRDD.options
    oos.defaultWriteObject()
  }

  override def compute(split: Partition, context: TaskContext): Iterator[(Long, Iterator[SearchRecord[H]])] = {
    val matchPartition = split.asInstanceOf[MatchRDDPartition]

    parent[H](0).iterator(matchPartition.searchPartition, context)

    // Match other partitions against our
    tryAndClose(parent[H](0).asInstanceOf[SearchRDD[H]].reader(matchPartition.searchPartition.index, matchPartition.searchPartition.indexDir)) {
      spr =>
        parent[(Long, S)](1).iterator(matchPartition.otherPartition, context)
          .map(docIndex => (docIndex._1,
            try {
              spr.search(queryBuilder(docIndex._2), topK, minScore).map(searchRecordJavaToProduct).toSeq.iterator
            } catch {
              case e: SearchException => throw new SearchException("error during querying from " + docIndex._1 + ": " + docIndex._2, e)
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
    val array = new Array[Partition](searchRDD.partitions.length * other.partitions.length)
    for (s1 <- parent[H](0).partitions; s2 <- parent[(Long, S)](1).partitions) {
      val idx = s1.index * numPartitionsInOther + s2.index
      array(idx) = new MatchRDDPartition(idx, s1.index, s2.index, searchRDD, other)
    }
    array
  }

  override def getDependencies: Seq[Dependency[_]] = List(
    new NarrowDependency(searchRDD) {
      def getParents(id: Int): Seq[Int] = List(id / numPartitionsInOther)
    },
    new NarrowDependency(other) {
      def getParents(id: Int): Seq[Int] = List(id % numPartitionsInOther)
    }
  )

  private def waitForIndex(matchRDDPartition: MatchRDDPartition): Unit = {
    breakable {
      while (true) {
        var dr: DirectoryReader = null
        try {
          logInfo(s"Waiting for part ${matchRDDPartition.index} directory ${matchRDDPartition.searchPartition.indexDir} on part ${matchRDDPartition.searchPartitionIndex} other part ${matchRDDPartition.otherPartitionIndex}")
          dr = DirectoryReader.open(options
            .getReaderOptions
            .indexDirectoryProvider
            .create(Paths.get(matchRDDPartition.searchPartition.indexDir)))
          val idex = new IndexSearcher(dr)
          idex.count(new MatchAllDocsQuery)
          break
        } catch {
          case _: IOException => Thread.sleep(10)
        } finally {
          if (dr != null)
            dr.close()
        }
      }
    }
  }

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
