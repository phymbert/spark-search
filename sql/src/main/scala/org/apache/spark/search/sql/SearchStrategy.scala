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

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.{SparkPlan, SparkStrategy}

/**
 * Search strategy.
 *
 * @author Pierrick HYMBERT
 */
object SearchStrategy extends SparkStrategy {

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = {
    plan match {
      case SearchJoin(left, right, searchExpression) => SearchJoinExec(planLater(left), planLater(right), searchExpression):: Nil
      case p: SearchIndexPlan =>
        SearchRDDExec(planLater(p.child), p.searchExpression) :: Nil
      case _ => Seq.empty
    }
  }

}
