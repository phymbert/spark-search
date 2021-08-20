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

import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.search.MatchJava;
import org.apache.spark.search.SearchRecordJava;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class SearchRDDJavaTest {
    static JavaSparkContext sc;

    @BeforeAll
    static void setupSpark() {
        sc = new JavaSparkContext(new SparkContext("local[*]", "SearchRDDJavaTest"));
        sc.setLogLevel("WARN");
    }

    @Test
    public void shouldCount() {
        JavaRDD<PersonJava> persons = sc.parallelize(PersonJava.PERSONS);

        SearchRDDJava<PersonJava> searchRDD = SearchRDDJava.of(persons, PersonJava.class);

        assertEquals(5, searchRDD.count());
        assertEquals(1, searchRDD.count("firstName:agnes~0.5"));
    }

    @Test
    public void shouldSearchList() {
        JavaRDD<PersonJava> persons = sc.parallelize(PersonJava.PERSONS).repartition(1);

        SearchRDDJava<PersonJava> searchRDD = SearchRDDJava.of(persons, PersonJava.class);

        assertEquals(5, searchRDD.count());

        assertEquals(new SearchRecordJava<>(4, 0,
                        0.4378082752227783f, 0, PersonJava.PERSONS.get(4)),
                searchRDD.searchList("firstName:agnes~0.5", 1, 0)[0]);
    }

    @Test
    public void shouldSearchJoin() {
        JavaRDD<PersonJava> persons = sc.parallelize(PersonJava.PERSONS).repartition(1);
        JavaRDD<PersonJava> persons2 = sc.parallelize(PersonJava.PERSONS2).repartition(1);

        SearchRDDJava<PersonJava> searchRDD = SearchRDDJava.of(persons, PersonJava.class);
        JavaRDD<MatchJava<PersonJava, PersonJava>> matches = searchRDD.searchJoin(persons2,
                doc -> Stream.of(
                                Optional.ofNullable(doc.getFirstName()).map(fn -> String.format("(firstName:%s~0.5)", fn)),
                                Optional.ofNullable(doc.getLastName()).map(ln -> String.format("(lastName:%s~0.5)", ln))
                        )
                        .filter(Optional::isPresent)
                        .map(Optional::get)
                        .collect(Collectors.joining(" AND ")), 10, 0.4);

        assertEquals(5, matches.count());

        matches.foreach(m -> assertEquals(1, m.hits.length));
    }

    @Test
    public void shouldSaveAndReload() throws IOException {
        FileUtils.deleteDirectory(new File("target/test-save"));

        SearchRDDJava.of(sc.parallelize(PersonJava.PERSONS).repartition(1), PersonJava.class)
                        .save("target/test-save");

        SearchRDDJava<PersonJava> restoredSearchRDD = SearchRDDJava
                .load(sc, "target/test-save", PersonJava.class);

        System.err.println("TESSSST: " + restoredSearchRDD.count());
        System.err.println("TESSSST: " + restoredSearchRDD.count("firstName:lastName"));
        System.err.println("TESSSST: " + restoredSearchRDD.count("firstName:lastName"));

        restoredSearchRDD.search("lastName:Julia~0.4", 1, 0).collect().forEach(System.err::println);

        assertEquals(Optional.of(4),
                restoredSearchRDD.search("firstName:Julia~0.4", 1, 0)
                        .map(SearchRecordJava::getSource)
                        .map(PersonJava::getAge)
                        .collect().stream().findFirst());
    }
}
