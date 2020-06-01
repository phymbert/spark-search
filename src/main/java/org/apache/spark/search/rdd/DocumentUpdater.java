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

import org.apache.lucene.document.Document;

import java.io.Serializable;

/**
 * Update the lucene {@link org.apache.lucene.document.Document} with the next element to be indexed.
 */
@FunctionalInterface
public interface DocumentUpdater<T> extends Serializable {
    /**
     * Next indexing document state.
     */
    class IndexingDocument<T> {
        /**
         * Input raw document to convert.
         */
        public T element;
        /**
         * Current lucene document to update.
         */
        public final Document doc = new Document();
        /**
         * Indexation options.
         */
        public final IndexationOptions<T> options;

        IndexingDocument(IndexationOptions<T> options) {
            this.options = options;
        }
    }

    /**
     * Update the lucene document and fields according to the current element.
     * According to https://cwiki.apache.org/confluence/display/LUCENE/ImproveIndexingSpeed
     * the document must just be updated for better performances.
     * Implementations must avoid instantiate object during indexation to save substantial GC cost.
     *
     * @param indexingDocument Next indexing document state
     */
    void update(IndexingDocument<T> indexingDocument) throws Exception;
}
