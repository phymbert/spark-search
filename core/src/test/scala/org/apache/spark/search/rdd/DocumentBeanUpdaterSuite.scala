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
package org.apache.spark.search.rdd

import org.apache.spark.search.reflect.DocumentBeanUpdater
import org.apache.spark.search._
import org.scalatest.funsuite.AnyFunSuite

class DocumentBeanUpdaterSuite extends AnyFunSuite {

  test("document bean updater should support case class") {
    val documentBeanUpdater = new DocumentBeanUpdater[TestData.Person]
    val indexingDocument = new DocumentUpdater.IndexingDocument[TestData.Person](IndexationOptions.defaultOptions
      .asInstanceOf[IndexationOptions[TestData.Person]])
    val doc = indexingDocument.doc

    indexingDocument.element = TestData.Person("John", "Doe", 34)

    documentBeanUpdater.update(indexingDocument)

    assertResult(3)(doc.getFields.size)
    assertResult("John")(doc.get("firstName"))
  }
}
