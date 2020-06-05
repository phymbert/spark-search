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

import org.apache.lucene.search.Query;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.search.SearchRecordJava;

import java.io.Serializable;

/**
 * Definition of search RDD for java.
 */
public interface ISearchRDDJava<T> {
    /**
     * {@link org.apache.spark.search.rdd.SearchRDD#count()}
     */
    long count();

    /**
     * {@link org.apache.spark.search.rdd.SearchRDD#count(org.apache.lucene.search.Query)}
     */
    long count(String query);

    /**
     * {@link org.apache.spark.search.rdd.SearchRDD#searchList(org.apache.lucene.search.Query, int, double)}
     */
    SearchRecordJava<T>[] searchList(String query, int topK);

    /**
     * {@link org.apache.spark.search.rdd.SearchRDD#searchList(org.apache.lucene.search.Query, int, double)}
     */
    SearchRecordJava<T>[] searchList(String query, int topK, double minScore);

    /**
     * {@link org.apache.spark.search.rdd.SearchRDD#search(org.apache.lucene.search.Query, int, double)}
     */
    JavaRDD<SearchRecordJava<T>> search(String query, int topK, double minScore);

    /**
     * Build a lucene query string to search for matching hits
     * against the input bean.
     */
    @FunctionalInterface
    interface QueryStringBuilder<T> extends Serializable {
        String build(T doc);
    }

    /**
     * Build a lucene query to search for matching hits
     * against the input bean.
     */
    @FunctionalInterface
    interface QueryBuilder<T> extends Serializable {
        Query build(T doc);
    }
}
