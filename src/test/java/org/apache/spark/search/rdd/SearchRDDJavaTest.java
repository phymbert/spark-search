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
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.search.SearchRecordJava;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class SearchRDDJavaTest {
    private static final SparkSession spark = SparkSession.builder()
            .master("local[*]")
            .getOrCreate();
    private static final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

    @Test
    public void searchRDDShouldBeUsableInJava() {
        JavaRDD<PersonJava> persons = sc.parallelize(PersonJava.PERSONS);

        SearchRDDJava<PersonJava> searchRDD = new SearchRDDJava<>(persons);

        assertEquals(5, searchRDD.count());

        assertEquals(new SearchRecordJava<>(1, 3, 0.2520535f, 0, PersonJava.PERSONS.get(4)),
                searchRDD.searchList("firstName:agnes~0.5", 1,0)[0]);
    }
}
