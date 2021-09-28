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
import org.apache.spark.search.rdd.SearchRDDImpl
import org.apache.spark.search.{IndexationOptions, ReaderOptions, SearchOptions, _}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression, Literal, UnsafeRow}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.types.{DataTypes, StringType, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String


case class SearchJoinExec(left: SparkPlan, right: SparkPlan, searchExpression: Expression)
  extends BinaryExecNode {

  override protected def doExecute(): RDD[InternalRow] = {
    val leftRDD = left.execute()
    val searchRDD = right.execute().asInstanceOf[SearchRDDImpl[InternalRow]]
    val opts = searchRDD.options

    val qb = searchExpression match { // FIXME support AND / OR
      case MatchesExpression(left, right) => right match {
        case Literal(value, dataType) =>
          dataType match {
            case StringType => left match {
              case a: AttributeReference =>
                queryBuilder[InternalRow]((_: InternalRow, lqb: QueryBuilder) =>
                  lqb.createBooleanQuery(a.name, value.asInstanceOf[UTF8String].toString), opts)
              case _ => throw new UnsupportedOperationException
            }
            case _ => throw new UnsupportedOperationException
          }
        case _ => throw new UnsupportedOperationException
      }
      case _ => throw new IllegalArgumentException
    }

    searchRDD.matchesQuery(leftRDD.zipWithIndex().map(_.swap), qb, 1)
      .filter(_._2._2.nonEmpty) // TODO move this filter at partition level
      .values.map(m =>
        toRow(m._1.asInstanceOf[UnsafeRow],
          m._2.head.score))
  }

  private def toRow(doc: UnsafeRow, score: Double): InternalRow = {
    val row = new UnsafeRow(doc.numFields + 1)
    val bs = new Array[Byte](row.getSizeInBytes)
    row.pointTo(bs, bs.length)
    row.copyFrom(doc)
    row.setDouble(doc.numFields, score)
    row
  }

  override def output: Seq[Attribute] = left.output ++ Seq(scoreAttribute)
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

    new SearchRDDImpl[InternalRow](rdd, opts)
  }

  override def output: Seq[Attribute] = Seq()
}