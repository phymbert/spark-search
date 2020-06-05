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

package org.apache.spark.search;

import org.apache.lucene.document.Document;
import org.apache.lucene.search.ScoreDoc;

import java.io.Serializable;

/**
 * Convert scored and lucene documents to search record.
 */
@FunctionalInterface
public interface DocumentConverter<T> extends Serializable {

    SearchRecordJava<T> convert(int partitionIndex, ScoreDoc scoreDoc, Class<T> classTag, Document doc) throws Exception;
}
