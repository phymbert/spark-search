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

import org.apache.spark.rdd.RDD;

import java.io.Serializable;
import java.util.Objects;

/**
 * Search record.
 */
public class SearchRecord<T> implements Serializable {

    private static final long serialVersionUID = 6220753751555421030L;

    /**
     * A hit document's number.
     * It is unique by partition only.
     *
     * @see org.apache.lucene.search.ScoreDoc#doc
     */
    private final int id;

    /**
     * RDD Partition index.
     *
     * @see RDD#id()
     */
    private final int partitionIndex;

    /**
     * The score of this document for the query.
     *
     * @see org.apache.lucene.search.ScoreDoc#score
     */
    private final float score;

    /**
     * Lucene shard index.
     *
     * @see org.apache.lucene.search.ScoreDoc#shardIndex
     */
    private final int shardIndex;

    /**
     * Source document.
     */
    private final T source;

    public SearchRecord(int id, int partitionIndex, float score, int shardIndex, T source) {
        this.id = id;
        this.partitionIndex = partitionIndex;
        this.score = score;
        this.shardIndex = shardIndex;
        this.source = source;
    }

    public int getId() {
        return id;
    }

    public int getPartitionIndex() {
        return partitionIndex;
    }

    public float getScore() {
        return score;
    }

    public int getShardIndex() {
        return shardIndex;
    }

    public T getSource() {
        return source;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SearchRecord<?> that = (SearchRecord<?>) o;
        return id == that.id &&
                partitionIndex == that.partitionIndex &&
                Float.compare(that.score, score) == 0 &&
                shardIndex == that.shardIndex &&
                source.equals(that.source);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, partitionIndex, score, shardIndex, source);
    }

    @Override
    public String toString() {
        return "SearchRecord{" +
                "id=" + id +
                ", partitionIndex=" + partitionIndex +
                ", score=" + score +
                ", shardIndex=" + shardIndex +
                ", source=" + source +
                '}';
    }
}
