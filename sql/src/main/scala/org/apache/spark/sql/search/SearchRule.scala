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

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical
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
    case logical.Join(left, right, joinType, condition, hint) if condition.exists(hasSearchCondition) =>
      SearchJoin(left, SearchIndexPlan(right), joinType, condition, hint)
  }

  private def hasSearchCondition(e: Expression): Boolean = {
    e match {
      case _: MatchesExpression => true
      case _ => e.children.exists(hasSearchCondition)
    }
  }
}
