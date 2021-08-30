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
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.search.MatchJava;
import org.apache.spark.search.SearchException;
import org.apache.spark.search.SearchOptions;
import org.apache.spark.search.SearchRecordJava;
import scala.reflect.ClassTag;

import java.io.Serializable;

/**
 * Definition of search RDD for java.
 */
public interface SearchRDDJava<T> {
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
     * @param <S>          Doc type to match with
     * @return Searched matches documents RDD
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
     * @param <S>          Doc type to match with
     * @return Searched matches documents RDD
     */
    <S> JavaRDD<MatchJava<S, T>> searchJoinQuery(JavaRDD<S> rdd,
                                                 QueryBuilder<S> queryBuilder,
                                                 int topK,
                                                 double minScore);

    /**
     * Saves the current indexed RDD onto hdfs
     * in order to be able to reload it later on.
     *
     * @param path Path on the spark file system (hdfs) to save on
     */
    void save(String path);

    /**
     * Returns this search RDD as a classical search RDD.
     *
     * @return A classical search RDD
     */
    JavaRDD<T> javaRDD();

    /**
     * Builds a lucene query string to search for matching hits
     * against the input bean.
     */
    @FunctionalInterface
    interface QueryStringBuilder<T> extends Serializable {
        String build(T doc);
    }

    /**
     * Builds a lucene query to search for matching hits
     * against the input bean.
     */
    @FunctionalInterface
    interface QueryBuilder<T> extends Serializable {
        Query build(T doc);
    }

    /**
     * Builder to build a search java rdd.
     *
     * @param <T> Runtime type of document to index
     * @return A search RDD builder
     */
    static <T> Builder<T> builder() {
        return new Builder<>();
    }

    /**
     * Creates a search java rdd.
     *
     * @param rdd   RDD to index and search on
     * @param clazz Runtime time of the index documents
     * @param <T>   Runtime type of document to index
     * @return A search RDD builder
     */
    static <T> SearchRDDJava<T> of(JavaRDD<T> rdd, Class<T> clazz) {
        return SearchRDDJava.<T>builder().rdd(rdd).runtimeClass(clazz).build();
    }

    /**
     * Reload an indexed RDD from spark FS.
     *
     * @param sc    Spark context
     * @param path  Path where the search rdd lucene indices were previously saved
     * @param clazz Runtime class instance T of indexed document
     * @param <T>   Runtime class of indexed document
     * @return Reloaded search java rdd
     */
    static <T> SearchRDDJava<T> load(JavaSparkContext sc, String path, Class<T> clazz) {
        return load(sc, path, clazz, SearchOptions.defaultOptions());
    }

    /**
     * Reload an indexed RDD from spark FS.
     *
     * @param sc      Spark context
     * @param path    Path where the search rdd lucene indices were previously saved
     * @param clazz   Runtime class instance T of indexed document
     * @param options Search option
     * @param <T>     Runtime class of indexed document
     * @return Reloaded search java rdd
     */
    static <T> SearchRDDJava<T> load(JavaSparkContext sc, String path,
                                     Class<T> clazz, SearchOptions<T> options) {
        try {
            return (SearchRDDJava<T>) SearchRDDJava.class.getClassLoader()
                    .loadClass("org.apache.spark.search.rdd.SearchRDDReloadedJava")
                    .getDeclaredMethod("load", SparkContext.class, String.class,
                            SearchOptions.class, ClassTag.class)
                    .invoke(
                            null,
                            sc.sc(), path,
                            options, scala.reflect.ClassTag$.MODULE$.apply(clazz));
        } catch (Exception e) {
            throw new SearchException("Unable to reload SearchRDDJava from path "
                    + path + ", got: " + e, e);
        }
    }

    class Builder<T> {
        private JavaRDD<T> rdd;
        private Class<T> clazz;
        private SearchOptions<T> options = SearchOptions.defaultOptions();

        private Builder() {
        }

        public Builder<T> runtimeClass(Class<T> clazz) {
            this.clazz = clazz;
            return this;
        }

        public Builder<T> rdd(JavaRDD<T> rdd) {
            this.rdd = rdd;
            return this;
        }

        public Builder<T> options(SearchOptions<T> options) {
            this.options = options;
            return this;
        }

        public SearchRDDJava<T> build() {
            if (rdd == null) {
                throw new SearchException("Please specify rdd to search for");
            }
            if (clazz == null) {
                throw new SearchException("Please specify runtime class of element to search for");
            }
            return new SearchRDDJava2Scala<>(rdd, clazz, options);
        }
    }
}
