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
