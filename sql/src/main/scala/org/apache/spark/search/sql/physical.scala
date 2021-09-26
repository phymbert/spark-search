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
package org.apache.spark.search.sql

import org.apache.lucene.util.QueryBuilder
import org.apache.spark.rdd.RDD
import org.apache.spark.search.rdd.{SearchRDDImpl, SearchRDDIndexer}
import org.apache.spark.search.{IndexationOptions, ReaderOptions, SearchOptions, _}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, GenerateUnsafeRowJoiner}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression, JoinedRow, Literal, Predicate, UnsafeRow}
import org.apache.spark.sql.catalyst.plans.{JoinType, LeftOuter}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.joins.{BaseJoinExec, UnsafeCartesianRDD}
import org.apache.spark.sql.types.{DataTypes, StringType, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String


case class SearchJoinExec(left: SparkPlan, right: SparkPlan, searchExpression: Expression)
  extends BaseJoinExec with CodegenSupport {
  override def joinType: JoinType = LeftOuter // FIXME always sql left join for now

  override def leftKeys: Seq[Expression] = Seq()

  override def output: Seq[Attribute] = left.output ++ right.output.map(_.withNullability(true)) // FIXME output always sql left join for now

  override def condition: Option[Expression] = Some(searchExpression)

  override def rightKeys: Seq[Expression] = Seq()

  override def inputRDDs(): Seq[RDD[InternalRow]] = left.execute() :: right.execute() :: Nil

  override protected def doProduce(ctx: CodegenContext): String = {
    "throw new UnsuportedOperationException();"
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

case class SearchRDDExec(child: SparkPlan, searchExpression: Expression)
  extends UnaryExecNode {

  override protected def doExecute(): RDD[InternalRow] = {
    val inputRDDs = child match {
      case wsce: WholeStageCodegenExec => wsce.child.asInstanceOf[CodegenSupport].inputRDDs()
      case cs: CodegenSupport => cs.inputRDDs()
      case _ => throw new UnsupportedOperationException("no input rdd supported")
    }

    if (inputRDDs.length != 1) {
      throw new UnsupportedOperationException("one input RDD expected")
    }

    val rdd = inputRDDs.head

    val schema = StructType(Seq(searchExpression match { // FIXME support AND / OR
      case MatchesExpression(left, _) => left match {
        case a: AttributeReference =>
          StructField(a.name, DataTypes.StringType)
        case _ => throw new UnsupportedOperationException
      }
      case _ => throw new IllegalArgumentException
    }))

    val opts = SearchOptions.builder[InternalRow]()
      .read((readOptsBuilder: ReaderOptions.Builder[InternalRow]) => readOptsBuilder.documentConverter(new DocumentRowConverter(schema)))
      .index((indexOptsBuilder: IndexationOptions.Builder[InternalRow]) => indexOptsBuilder.documentUpdater(new DocumentRowUpdater(schema)))
      .build()

    new SearchRDDIndexer[InternalRow](rdd, opts)
  }

  override def output: Seq[Attribute] = Seq()
}