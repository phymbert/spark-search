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

package org.apache.spark.search.rdd;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Matching result of a document against a SearchRDD.
 * @param <S> Type of the bean from which the query was built
 * @param <H> Result hits type
 */
public class Match<S, H> implements Serializable {
    private static final long serialVersionUID = 1L;

    /**
     * Original RDD index where the doc come from.
     */
    private final long docIndex;

    /**
     * Original document which originated the query.
     */
    private final S doc;

    /**
     * TopK hits matching the source document for that query.
     */
    private List<SearchRecord<H>> hits = new ArrayList<>();

    public Match(long docIndex, S doc) {
        this.docIndex = docIndex;
        this.doc = doc;
    }

    public Match(long docIndex, S doc, List<SearchRecord<H>> hits) {
        this.docIndex = docIndex;
        this.doc = doc;
        this.hits = hits;
    }

    public long getDocIndex() {
        return docIndex;
    }

    public S getDoc() {
        return doc;
    }

    public List<SearchRecord<H>> getHits() {
        return hits;
    }

    public void setHits(List<SearchRecord<H>> hits) {
        this.hits = hits;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Match<?, ?> match = (Match<?, ?>) o;
        return docIndex == match.docIndex;
    }

    @Override
    public int hashCode() {
        return Objects.hash(docIndex);
    }

    @Override
    public String toString() {
        return "Match{" +
                "docIndex=" + docIndex +
                ", doc=" + doc +
                ", hits=" + hits +
                '}';
    }
}
