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

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression, ExpressionSet, PredicateHelper}
import org.apache.spark.sql.catalyst.plans.{ExistenceJoin, FullOuter, InnerLike, JoinType, LeftExistence, LeftOuter, LeftSemi, RightOuter}
import org.apache.spark.sql.catalyst.plans.logical.{BinaryNode, Join, JoinHint, LeafNode, LogicalPlan}
import org.apache.spark.sql.types.DoubleType

trait SearchLogicalPlan

case class SearchJoin(left: LogicalPlan,
                      right: SearchIndexPlan,
                      joinType: JoinType,
                      condition: Option[Expression],
                      hint: JoinHint)
  extends
    BinaryNode with PredicateHelper {

  override def output: Seq[Attribute] =
    joinType match {
      case LeftOuter => left.output ++
        right.output.map(_.withNullability(true)) ++
        Seq(AttributeReference(SCORE, DoubleType, nullable = false)())
      case _ => throw new UnsupportedOperationException()
    }

  override def metadataOutput: Seq[Attribute] = {
    joinType match {
      case LeftOuter =>
        children.flatMap(_.metadataOutput)
      case _ => throw new UnsupportedOperationException()
    }
  }

  override protected lazy val validConstraints: ExpressionSet = {
    joinType match {
      case LeftOuter =>
        left.constraints
      case _ => throw new UnsupportedOperationException()
    }
  }
}

case class SearchIndexPlan(child: LogicalPlan)
  extends LeafNode
    with SearchLogicalPlan {

  override def output: Seq[Attribute] = child.output
}
