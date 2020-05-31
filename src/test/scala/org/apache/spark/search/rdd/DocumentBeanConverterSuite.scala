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

package org.apache.spark.search.rdd

import org.apache.lucene.document.{Document, Field, StringField}
import org.apache.lucene.search.ScoreDoc
import org.scalatest.funsuite.AnyFunSuite

class DocumentBeanConverterSuite extends AnyFunSuite {

  test("document bean converter should support case class") {
    val converter = new DocumentBeanConverter[TestData.Person]
    converter.setClassTag(classOf[TestData.Person])

    val scoreDoc: ScoreDoc = new ScoreDoc(1, 2f, 3)
    val doc: Document = new Document
    doc.add(new StringField("firstName", "Joe", Field.Store.YES))
    doc.add(new StringField("lastName", "Duck", Field.Store.YES))
    doc.add(new StringField("age", "32", Field.Store.YES))

    val searchRecord: SearchRecord[TestData.Person] = converter.convert(4, scoreDoc, doc)
    assertResult(1)(searchRecord.getId)
    assertResult(2f)(searchRecord.getScore)
    assertResult(3)(searchRecord.getShardIndex)
    assertResult(4)(searchRecord.getPartitionIndex)

    val person: TestData.Person = searchRecord.getSource
    assertResult("Joe")(person.firstName)
    assertResult("Duck")(person.lastName)
    assertResult(32)(person.age)
  }
}