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
        SearchRDDExec(planLater(p.child), p.output) :: Nil
      case _ => Seq.empty
    }
  }

}
