/*
 *    Copyright 2020 the Spark Search contributors
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package org.apache.spark.search.sql

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenerator, CodegenContext, ExprCode, FalseLiteral, JavaCode}
import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, Expression, LeafExpression}
import org.apache.spark.sql.types.{DataType, _}

trait SearchExpression

case class ScoreExpression() extends LeafExpression
  with SearchExpression {

  override def toString: String = s"SCORE"

  override def dataType: DataType = DoubleType

  override def nullable: Boolean = false

  override def eval(input: InternalRow): Any = {
    if (input != null) input.getDouble(input.numFields - 1) else Double.NaN
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    if (ctx.currentVars != null && ctx.currentVars.last != null) {
      val oev = ctx.currentVars.last
      ev.isNull = oev.isNull
      ev.value = oev.value
      ev.copy(code = oev.code)
    } else {
      assert(ctx.INPUT_ROW != null, "INPUT_ROW and currentVars cannot both be null.")
      val javaType = JavaCode.javaType(dataType)
      ev.copy(code = code"$javaType ${ev.value} = ${ctx.INPUT_ROW}.getDouble(i.numFields() - 1);", isNull = FalseLiteral)
    }
  }
}

/**
 * @author Pierrick HYMBERT
 */
case class MatchesExpression(left: Expression, right: Expression)
  extends BinaryExpression
    with SearchExpression {

  override def toString: String = s"$left MATCHES $right"

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    ev.copy(code =
      code"""
      final ${CodeGenerator.javaType(dataType)} ${ev.value} = true;""", isNull = FalseLiteral)
  }

  override def dataType: DataType = BooleanType
}

