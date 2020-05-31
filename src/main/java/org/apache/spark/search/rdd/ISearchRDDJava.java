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

import java.util.List;

/**
 * Definition of search RDD for java.
 */
public interface ISearchRDDJava<T> {
    /**
     * {@link org.apache.spark.rdd.RDD#count()}
     */
    long count();

    /**
     * {@link org.apache.spark.search.rdd.SearchRDD#count(java.lang.String)}
     */
    Long count(String query);

    /**
     * {@link org.apache.spark.search.rdd.SearchRDD#search(java.lang.String, int)}
     * @return
     */
    List<SearchRecord<T>> search(String query, Integer topK);
}
