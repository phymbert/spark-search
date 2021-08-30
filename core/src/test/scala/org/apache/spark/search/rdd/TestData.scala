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

import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.analysis.Analyzer.TokenStreamComponents
import org.apache.lucene.analysis.ngram.NGramTokenizer

object TestData {

  case class Person(firstName: String, lastName: String, age: Int)

  def persons = Seq(
    Person("Geoorge", "Michael", 53),
    Person("Bob", "Marley", 37),
    Person("Agnès", "Bartoll", -1))

  def personsDuplicated = Seq(
    Person("George", "Michal", 0),
    Person("Georgee", "Michall", 0),
    Person("Bobb", "Marley", 0),
    Person("Bob", "Marlley", 0),
    Person("Agnes", "Bartol", 0),
    Person("Agnec", "Barttol", 0))

  class TestPersonAnalyzer extends Analyzer {
    override def createComponents(fieldName: String): TokenStreamComponents = {
      new TokenStreamComponents(new NGramTokenizer(1, 3))
    }
  }

}
