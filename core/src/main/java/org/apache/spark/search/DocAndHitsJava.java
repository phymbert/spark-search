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
package org.apache.spark.search;

import java.io.Serializable;
import java.util.Objects;

/**
 * Matching result of a document against a SearchRDD.
 *
 * @param <S> Type of the bean from which the query was built
 * @param <H> Result hits type
 */
public class DocAndHitsJava<S, H> implements Serializable {
    private static final long serialVersionUID = 1L;

    /**
     * Original document which originated the query.
     */
    public S doc;

    /**
     * TopK hits matching the source document for that query.
     */
    public SearchRecordJava<H>[] hits;

    public DocAndHitsJava() {
    }

    public DocAndHitsJava(S doc, SearchRecordJava<H>[] hits) {
        this.doc = doc;
        this.hits = hits;
    }

    public S getDoc() {
        return doc;
    }

    public SearchRecordJava<H>[] getHits() {
        return hits;
    }

    public void setDoc(S doc) {
        this.doc = doc;
    }

    public void setHits(SearchRecordJava<H>[] hits) {
        this.hits = hits;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DocAndHitsJava<?, ?> match = (DocAndHitsJava<?, ?>) o;
        return doc.equals(match.doc) &&
                hits.equals(match.hits);
    }

    @Override
    public int hashCode() {
        return Objects.hash(doc, hits);
    }

    @Override
    public String toString() {
        return "Match{" +
                "doc=" + doc +
                ", hits=" + hits +
                '}';
    }
}
