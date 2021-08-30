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

import org.apache.spark.search.sql.TestData._
import org.scalatest.flatspec.AnyFlatSpec

class SearchDatasetSuite extends AnyFlatSpec with LocalSparkSession {

  ignore should "be searchable" in {
    val spark = _spark
    import spark.sqlContext.implicits._

    val appleCompany = companies1DS(spark)
      .where($"name".matches("apple") && score() > 1d)

    assertResult(Company("Apple, Inc")) {
      appleCompany.collect()
    }
  }
}
