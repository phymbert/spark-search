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
package org.apache.spark.sql.search

import org.apache.spark.rdd.RDD
import org.apache.spark.search.rdd.{SearchRDDImpl, SearchRDDIndexer}
import org.apache.spark.search.{IndexationOptions, ReaderOptions, SearchOptions}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, GenerateUnsafeRowJoiner}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression, JoinedRow, Predicate, UnsafeRow}
import org.apache.spark.sql.catalyst.plans.{InnerLike, JoinType, LeftOuter}
import org.apache.spark.sql.catalyst.plans.logical.JoinHint
import org.apache.spark.sql.catalyst.plans.physical.{Partitioning, PartitioningCollection}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.joins.{BaseJoinExec, UnsafeCartesianRDD}
import org.apache.spark.sql.types.DoubleType


case class SearchJoinExec(left: SparkPlan,
                          right: SparkPlan,
                          joinType: JoinType,
                          condition: Option[Expression],
                          hint: JoinHint)
  extends BaseJoinExec with CodegenSupport {

  override def leftKeys: Seq[Expression] = Seq()

  override def output: Seq[Attribute] = left.output ++
    right.output.map(_.withNullability(true)) ++ // FIXME output always sql left join for now
    Seq(AttributeReference(SCORE, DoubleType, nullable = false)())

  override def rightKeys: Seq[Expression] = Seq()

  override def inputRDDs(): Seq[RDD[InternalRow]] = left.execute() :: right.execute() :: Nil

  override def outputPartitioning: Partitioning = joinType match {
    case LeftOuter =>
      PartitioningCollection(Seq(left.outputPartitioning, right.outputPartitioning))
    case _ => throw new UnsupportedOperationException()
  }

  override protected def doProduce(ctx: CodegenContext): String = {
    // Inline mutable state since not many join operations in a task
    val leftInput = ctx.addMutableState("scala.collection.Iterator", "leftInput",
      v => s"$v = inputs[0];", forceInline = true)
    val rightInput = ctx.addMutableState("scala.collection.Iterator", "rightInput",
      v => s"$v = inputs[1];", forceInline = true)
    val leftRow = ctx.addMutableState("InternalRow", "leftRow", forceInline = true)
    val rightRow = ctx.addMutableState("InternalRow", "rightRow", forceInline = true)
    val reader = ctx.addMutableState("org.apache.spark.search.rdd.SearchPartitionReader", "reader", forceInline = true)

    s"""
       |
       | // Unzip if needed
       | org.apache.spark.search.rdd.ZipUtils.unzipPartition("/tmp/test", $rightInput);
       | $reader = new org.apache.spark.search.rdd.SearchPartitionReader(0,
       |                          "/tmp/test",
       |                          InternalRow.class,
       |                          org.apache.spark.search.ReaderOptions.DEFAULT);
       | $reader.close();
       |
       |""".stripMargin
  }

  override protected def doExecute(): RDD[InternalRow] = {

    val leftResults = left.execute().asInstanceOf[RDD[UnsafeRow]]
    val rightResults = right.execute().asInstanceOf[RDD[UnsafeRow]]

    val pair = new UnsafeCartesianRDD(
      leftResults,
      rightResults,
      right.output.size,
      0, //FIXME
      0)
    //FIXME)
    pair.mapPartitionsWithIndexInternal { (index, iter) =>
      val joiner = GenerateUnsafeRowJoiner.create(left.schema, right.schema)
      val boundCondition = Predicate.create(condition.get, left.output ++ right.output)
      boundCondition.initialize(index)
      val joined = new JoinedRow

      iter.filter { r =>
        boundCondition.eval(joined(r._1, r._2))
      }.map(r => joiner.join(r._1, r._2))
    }
  }
}

case class SearchIndexExec(child: SparkPlan)
  extends UnaryExecNode {

  override protected def doExecute(): RDD[InternalRow] = {
    val rdd = child.execute()

    val schema = child.schema

    val opts = SearchOptions.builder[InternalRow]()
      .read((readOptsBuilder: ReaderOptions.Builder[InternalRow]) => readOptsBuilder.documentConverter(new DocumentRowConverter(schema)))
      .index((indexOptsBuilder: IndexationOptions.Builder[InternalRow]) => indexOptsBuilder.documentUpdater(new DocumentRowUpdater(schema)))
      .build()

    new SearchRDDIndexer[InternalRow](rdd, opts)
      .map(a => {
        val row = new UnsafeRow(1)
        row.pointTo(a, 1)
        row
      })
  }

  override def output: Seq[Attribute] = child.output
}