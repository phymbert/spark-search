package org.apache.spark.search.sql

import org.apache.lucene.util.QueryBuilder
import org.apache.spark.rdd.RDD
import org.apache.spark.search.rdd.SearchRDD
import org.apache.spark.search.{IndexationOptions, ReaderOptions, SearchOptions, _}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression, GenericInternalRow, Literal}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String


case class SearchJoinExec(left: SparkPlan, right: SparkPlan, searchExpression: Expression)
  extends BinaryExecNode {


  override protected def doExecute(): RDD[InternalRow] = {
    val leftRDD = left.execute()
    val searchRDD = right.execute().asInstanceOf[SearchRDD[InternalRow]]
    val opts = searchRDD.options

    val qb = searchExpression match { // FIXME support AND / OR
      case MatchesExpression(left, right) => right match {
        case Literal(value, dataType) =>
          dataType match {
            case StringType => left match {
              case a: AttributeReference =>
                queryBuilder[InternalRow]((_: InternalRow, lqb: QueryBuilder) =>
                  lqb.createPhraseQuery(a.name, value.asInstanceOf[UTF8String].toString), opts)
              case _ => throw new UnsupportedOperationException
            }
            case _ => throw new UnsupportedOperationException
          }
        case _ => throw new UnsupportedOperationException
      }
      case _ => throw new IllegalArgumentException
    }

    searchRDD.searchQueryJoin(leftRDD, qb, 1)
      .filter(_.hits.nonEmpty) // TODO move this filter at partition level
      .map(m => InternalRow.fromSeq(m.doc.asInstanceOf[GenericInternalRow].values ++ Seq(m.hits.head.score)))
  }

  override def output: Seq[Attribute] = left.output ++ right.output
}

case class SearchRDDExec(child: SparkPlan, indexedAttributes: Seq[Attribute])
  extends UnaryExecNode {

  override protected def doExecute(): RDD[InternalRow] = {
    val inputRDDs = child.asInstanceOf[WholeStageCodegenExec].child.asInstanceOf[CodegenSupport].inputRDDs()

    if (inputRDDs.length > 1) {
      throw new UnsupportedOperationException("multiple RDDs not supported")
    }

    val rdd = inputRDDs.head


    val schema = StructType(indexedAttributes
      .filter(_.name != SCORE)
      .map(a => StructField(a.name, a.dataType, a.nullable)))

    val opts = SearchOptions.builder[InternalRow]()
      .read((readOptsBuilder: ReaderOptions.Builder[InternalRow]) => readOptsBuilder.documentConverter(new DocumentRowConverter(schema)))
      .index((indexOptsBuilder: IndexationOptions.Builder[InternalRow]) => indexOptsBuilder.documentUpdater(new DocumentRowUpdater(schema)))
      .build()

    new SearchRDD[InternalRow](rdd, opts)
  }

  override def output: Seq[Attribute] = indexedAttributes
    .filter(_.name == SCORE)
}