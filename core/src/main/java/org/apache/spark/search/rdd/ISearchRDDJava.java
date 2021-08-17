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

import org.apache.lucene.search.Query;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.search.MatchJava;
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
     * {@link org.apache.spark.search.rdd.SearchRDD#count(String)}
     */
    long count(String query);

    /**
     * {@link org.apache.spark.search.rdd.SearchRDD#searchList(String, int, double)}
     */
    SearchRecordJava<T>[] searchList(String query, int topK);

    /**
     * {@link org.apache.spark.search.rdd.SearchRDD#searchList(String, int, double)}
     */
    SearchRecordJava<T>[] searchList(String query, int topK, double minScore);

    /**
     * {@link org.apache.spark.search.rdd.SearchRDD#search(String, int, double)}
     */
    JavaRDD<SearchRecordJava<T>> search(String query, int topK, double minScore);

    /**
     * Searches and joins the input RDD matches against this one
     * by building a custom lucene query string per doc
     * and returns matching hits.
     *
     * @param rdd          to join with
     * @param queryBuilder builds the query string to join with the searched document
     * @param topK         topK to return
     * @param minScore     minimum score of matching documents
     * @return Searched matches documents RDD
     * @param <S> Doc type to match with
     */
    <S> JavaRDD<MatchJava<S, T>> searchJoin(JavaRDD<S> rdd,
                                                 QueryStringBuilder<S> queryBuilder,
                                                 int topK,
                                                 double minScore);

    /**
     * Searches and joins the input RDD matches against this one
     * by building a custom lucene query per doc
     * and returns matching hits.
     *
     * @param rdd          to join with
     * @param queryBuilder builds the lucene query to join with the searched document
     * @param topK         topK to return
     * @param minScore     minimum score of matching documents
     * @return Searched matches documents RDD
     * @param <S> Doc type to match with
     */
    <S> JavaRDD<MatchJava<S, T>> searchJoinQuery(JavaRDD<S> rdd,
                                                 QueryBuilder<S> queryBuilder,
                                                 int topK,
                                                 double minScore);


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
