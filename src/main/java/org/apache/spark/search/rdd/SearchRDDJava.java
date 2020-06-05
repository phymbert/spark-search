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

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.search.SearchOptions;
import org.apache.spark.search.SearchRecordJava;
import scala.reflect.ClassTag;

/**
 * Java friendly version of {@link SearchRDD}.
 */
public class SearchRDDJava<T> extends JavaRDD<T> implements ISearchRDDJava<T> {
    private static final long serialVersionUID = 1L;

    private final ISearchRDDJava<T> searchRDDJava;

    public SearchRDDJava(JavaRDD<T> rdd) {
        this(rdd, SearchOptions.defaultOptions());
    }

    public SearchRDDJava(JavaRDD<T> rdd, SearchOptions<T> options) {
        super(rdd.rdd(), scala.reflect.ClassTag$.MODULE$.apply(Object.class));
        try {
            // Yes, what a mess: JavaFirst was maybe not a good choice
            this.searchRDDJava
                    = (ISearchRDDJava) getClass().getClassLoader()
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
    public SearchRecordJava<T>[] searchList(String query, int topK) {
        return searchRDDJava.searchList(query, topK);
    }

    @Override
    public SearchRecordJava<T>[] searchList(String query, int topK, double minScore) {
        return searchRDDJava.searchList(query, topK, minScore);
    }

    @Override
    public JavaRDD<SearchRecordJava<T>> search(String query, int topK, double minScore) {
        return searchRDDJava.search(query, topK, minScore);
    }

    public static <T> SearchRDDJava<T> create(JavaRDD<T> rdd) {
        return new SearchRDDJava<>(rdd);
    }
}
