package org.apache.spark.search.sql

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{Column, SparkSession}

/**
 *
 * @author Pierrick HYMBERT
 */
class ColumnWithSearch(col: Column) {
  @transient private final val sqlContext = SparkSession.getActiveSession.map(_.sqlContext).orNull

  def matches(literal: String): Column = withSearchExpr {
    MatchesExpression(col.expr, lit(literal).expr)
  }

  def matches(other: Column): Column = withSearchExpr {
    MatchesExpression(col.expr, other.expr)
  }

  /** Creates a column based on the given expression. */
  private def withSearchExpr(newExpr: Expression): Column = {
    val searchSQLEnabled = sqlContext.experimental.extraStrategies.contains(SearchStrategy)

    if (!searchSQLEnabled) {
      sqlContext.experimental.extraStrategies = Seq(SearchStrategy) ++ sqlContext.experimental.extraStrategies
      sqlContext.experimental.extraOptimizations = Seq(SearchRule) ++ sqlContext.experimental.extraOptimizations
    }

    new Column(newExpr)
  }
}
