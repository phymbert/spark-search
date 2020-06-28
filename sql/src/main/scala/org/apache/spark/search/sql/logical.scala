package org.apache.spark.search.sql

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression}
import org.apache.spark.sql.catalyst.plans.logical.{BinaryNode, LeafNode, LogicalPlan, Statistics}
import org.apache.spark.sql.types.{DataTypes, DoubleType}

trait SearchLogicalPlan


case class SearchJoin(left: LogicalPlan, right: SearchIndexPlan, searchExpression: Expression)
  extends BinaryNode
    with SearchLogicalPlan {
  override def output: Seq[Attribute] = left.output ++ right.output
}


case class SearchIndexPlan(child: LogicalPlan, searchExpression: Expression)
  extends LeafNode
    with SearchLogicalPlan {

  override def output: Seq[Attribute] = Seq(AttributeReference(SCORE, DoubleType, nullable = false)())

  override def computeStats(): Statistics = Statistics(
    sizeInBytes = BigInt(Long.MaxValue) // Broadcast forbidden
  )
}
