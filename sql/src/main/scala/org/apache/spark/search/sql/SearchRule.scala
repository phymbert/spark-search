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

import org.apache.spark.sql.catalyst.expressions.{And, BinaryExpression, Expression, Or}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule

/**
 * Spark Search SQL rule.
 *
 * Rewrite logical plan involving search expression to their equivalent in search RDD.
 *
 * @author Pierrick HYMBERT
 */
object SearchRule extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = plan transformUp {
    case filter@Filter(conditions, _) if hasSearchCondition(conditions) =>
      extractSearchFilter(filter)
  }

  private def hasSearchCondition(e: Expression): Boolean = {
    e match {
      case _: MatchesExpression => true
      case _ => e.children.exists(hasSearchCondition)
    }
  }

  def extractSearchFilter(filter: Filter): LogicalPlan = {
    val (searchExpression, other) = searchConditions(filter.condition)

    val rddPlan = SearchIndexPlan(filter.child, searchExpression.get)
    val joinPlan = SearchJoin(filter.child, rddPlan, searchExpression.get)
    /*if (other.children.nonEmpty)
      Filter(other, joinPlan)
    else*/
      joinPlan // FIXME lost
  }

  private def searchConditions(e: Expression): (Option[Expression], Expression) = {
    e match {
      case m: MatchesExpression => (Option(e), m)
      case be: BinaryExpression =>
        val (leftSearchExpression, _) = searchConditions(be.left)
        val (rightSearchExpression, _) = searchConditions(be.right)
        val searchExpression = if (leftSearchExpression.nonEmpty && rightSearchExpression.nonEmpty)
          Option(be match {
            case _: And => And(leftSearchExpression.get, rightSearchExpression.get)
            case _: Or => Or(leftSearchExpression.get, rightSearchExpression.get)
            case _ => throw new UnsupportedOperationException
          })
        else if (leftSearchExpression.nonEmpty)
          leftSearchExpression
        else if (rightSearchExpression.nonEmpty)
          rightSearchExpression
        else Option.empty
        (searchExpression, be)
      case _ => (Option.empty, e)
    }
  }
}
