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

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.search.DocAndHitsJava;
import org.apache.spark.search.SearchOptions;
import org.apache.spark.search.SearchRecordJava;
import scala.reflect.ClassTag;

class SearchRDDJava2Scala<S> extends JavaRDD<S> implements SearchRDDJava<S> {
    private static final long serialVersionUID = 1L;

    private final SearchRDDJava<S> searchRDDJava;

    SearchRDDJava2Scala(JavaRDD<S> rdd, Class<S> clazz, SearchOptions<S> options) {
        super(rdd.rdd(), scala.reflect.ClassTag$.MODULE$.apply(clazz));
        try {
            // Yes, what a mess: JavaFirst was maybe not a good choice
            this.searchRDDJava
                    = (SearchRDDJava) getClass().getClassLoader()
                    .loadClass("org.apache.spark.search.rdd.SearchJavaBaseRDD")
                    .getConstructor(JavaRDD.class, SearchOptions.class, ClassTag.class)
                    .newInstance(rdd, options, classTag());
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public long count() {
        return searchRDDJava.count();
    }

    @Override
    public long count(String query) {
        return searchRDDJava.count(query);
    }

    @Override
    public SearchRecordJava<S>[] searchList(String query, int topK) {
        return searchRDDJava.searchList(query, topK);
    }

    @Override
    public SearchRecordJava<S>[] searchList(String query, int topK, double minScore) {
        return searchRDDJava.searchList(query, topK, minScore);
    }

    @Override
    public JavaRDD<SearchRecordJava<S>> search(String query, int topK, double minScore) {
        return searchRDDJava.search(query, topK, minScore);
    }

    @Override
    public <K, V> JavaRDD<DocAndHitsJava<V, S>> matches(JavaPairRDD<K, V> rdd,
                                                        QueryStringBuilder<V> queryBuilder,
                                                        int topK, double minScore) {
        return searchRDDJava.matches(rdd, queryBuilder, topK, minScore);
    }

    @Override
    public <K, V> JavaRDD<DocAndHitsJava<V, S>> matchesQuery(JavaPairRDD<K, V> rdd,
                                                             QueryBuilder<V> queryBuilder,
                                                             int topK, double minScore) {
        return searchRDDJava.matchesQuery(rdd, queryBuilder, topK, minScore);
    }

    @Override
    public void save(String path) {
        searchRDDJava.save(path);
    }

    @Override
    public JavaRDD<S> javaRDD() {
        return searchRDDJava.javaRDD();
    }

    @Override
    public RDD<S> rdd() {
        return javaRDD().rdd();
    }
}
