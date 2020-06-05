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
  override def apply(executionPlan: LogicalPlan): LogicalPlan = {
    executionPlan match {
      case agg@Aggregate(groupingExpressions, aggregateExpressions, aggregateChild) =>
        val (isRewritten, rewritten) = rewriteForSearch(aggregateChild)
        if (isRewritten) Aggregate(groupingExpressions, aggregateExpressions, rewritten) else agg
      case _ => executionPlan
    }
  }

  private def rewriteForSearch(logicalPlan: LogicalPlan): (Boolean, LogicalPlan) = {
    logicalPlan match {
      case project@Project(projectList, child) => child match {
        case Filter(conditions, childFilter) =>
          val (searchExpression, newConditions) = searchConditions(conditions)
          searchExpression.map(searchExpression => {
            val rddPlan = SearchIndexPlan(project, searchExpression)
            val joinPlan = SearchJoin(Project(projectList ++ Seq(scoreAttribute), Filter(newConditions, childFilter)), rddPlan, searchExpression)
            (true, joinPlan)
          }).getOrElse((false, project))
        case _ => (false, project)
      }
      case _ => (false, logicalPlan)
    }
  }

  private def searchConditions(e: Expression): (Option[Expression], Expression) = {
    e match {
      case m: MatchesExpression => (Option(e), m)
      case be: BinaryExpression =>
        val (leftSearchExpression, _) = searchConditions(be.left)
        val (rightSearchExpression, _) = searchConditions(be.right)
        val searchExpression = if (leftSearchExpression.nonEmpty && rightSearchExpression.nonEmpty)
          Option(be match {
            case _:And => And(leftSearchExpression.get, rightSearchExpression.get)
            case _:Or => Or(leftSearchExpression.get, rightSearchExpression.get)
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
