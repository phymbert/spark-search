package org.apache.spark.search.rdd

import java.io.{IOException, ObjectOutputStream}
import java.nio.file.Paths

import org.apache.lucene.index.DirectoryReader
import org.apache.lucene.search.{IndexSearcher, MatchAllDocsQuery, Query}
import org.apache.spark.rdd.RDD
import org.apache.spark.search.SearchRecord
import org.apache.spark.util.Utils
import org.apache.spark.{Dependency, NarrowDependency, Partition, TaskContext}

import scala.reflect.ClassTag
import scala.util.control.Breaks._

/**
 * Result RDD of a [[org.apache.spark.search.rdd.SearchRDD#searchQueryJoin(org.apache.spark.rdd.RDD, scala.Function1, int, double)]]
 *
 * @author Pierrick HYMBERT
 */
private[rdd] class MatchRDD[S: ClassTag, H: ClassTag](var searchRDD: SearchRDD[H],
                                                      var other: RDD[(Long, S)],
                                                      queryBuilder: S => Query,
                                                      topK: Int = Int.MaxValue,
                                                      minScore: Double = 0)
  extends RDD[(Long, Iterator[SearchRecord[H]])](searchRDD.context, Nil) {


  override def compute(split: Partition, context: TaskContext): Iterator[(Long, Iterator[SearchRecord[H]])] = {
    val matchPartition = split.asInstanceOf[MatchRDDPartition]

    if (matchPartition.otherPartitionIndex < 1) {
      dependencies.head.rdd.iterator(matchPartition.searchPartition, context)
    } else {
      waitForIndex(matchPartition.searchPartition.indexDir)
    }

    // Match other partitions against our
    tryAndClose(searchRDD.reader(matchPartition.searchPartition.index, matchPartition.searchPartition.indexDir)) {
      spr =>
        dependencies.last.rdd.asInstanceOf[RDD[(Long, S)]].iterator(matchPartition.otherPartition, context)
          .map(docIndex => (docIndex._1,
            spr.search(queryBuilder(docIndex._2), topK, minScore).map(searchRecordJavaToProduct).toSeq.iterator)
          ).toArray.toIterator
    }
  }

  override def clearDependencies() {
    super.clearDependencies()
    searchRDD = null
    other = null
  }

  private val numPartitionsInSearch = searchRDD.partitions.length
  private val numPartitionsInOther = other.partitions.length

  override protected def getPreferredLocations(split: Partition): Seq[String] =
    dependencies.head.rdd.asInstanceOf[SearchRDD[H]]
      .getPreferredLocations(split.asInstanceOf[MatchRDDPartition].searchPartition)

  override protected def getPartitions: Array[Partition] = {
    val array = new Array[Partition](searchRDD.partitions.length * other.partitions.length)
    for (s1 <- searchRDD.partitions; s2 <- other.partitions) {
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

  private def waitForIndex(indexDir: String): Unit = {
    breakable {
      while (true) {
        var dr: DirectoryReader = null
        try {
          dr = DirectoryReader.open(searchRDD.options.getReaderOptions.indexDirectoryProvider.create(Paths.get(indexDir)))
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

  private class MatchRDDPartition(val idx: Int,
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
