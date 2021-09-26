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
package org.apache.spark.search.rdd;

import org.apache.lucene.search.ScoreDoc;
import org.apache.spark.search.SearchRecordJava;

import java.util.Iterator;
import java.util.function.Function;

/**
 * Iterates over search records and close the search reader at the end.
 *
 * @author Pierrick HYMBERT
 */
class SearchRecordIterator<S> implements Iterator<SearchRecordJava<S>> {
    private final Function<ScoreDoc, SearchRecordJava<S>> converter;
    private final ScoreDoc[] scoreDocs;
    private int idx;

    SearchRecordIterator(Function<ScoreDoc, SearchRecordJava<S>> converter, ScoreDoc[] scoreDocs) {
        this.converter = converter;
        this.scoreDocs = scoreDocs;
    }

    @Override
    public boolean hasNext() {
        return idx < scoreDocs.length;
    }

    @Override
    public SearchRecordJava<S> next() {
        return converter.apply(scoreDocs[idx++]);
    }

    int size() {
        return scoreDocs.length;
    }
}
