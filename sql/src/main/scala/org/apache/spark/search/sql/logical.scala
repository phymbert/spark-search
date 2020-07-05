/**
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
