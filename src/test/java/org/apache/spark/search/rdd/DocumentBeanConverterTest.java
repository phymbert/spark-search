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
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.search.ScoreDoc;
import org.apache.spark.search.SearchRecordJava;
import org.apache.spark.search.reflect.DocumentBeanConverter;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class DocumentBeanConverterTest {

    @Test
    public void shouldConvertJavaBean() throws Exception {
        DocumentBeanConverter<PersonJava> converter = new DocumentBeanConverter<>();

        ScoreDoc scoreDoc = new ScoreDoc(1, 2f, 3);
        Document doc = new Document();
        doc.add(new StringField("firstName", "Joe", Field.Store.YES));
        doc.add(new StringField("lastName", "Duck", Field.Store.YES));
        doc.add(new StringField("age", "32", Field.Store.YES));

        SearchRecordJava<PersonJava> searchRecord = converter.convert(4, scoreDoc, PersonJava.class, doc);
        assertNotNull(searchRecord);
        assertNotNull(searchRecord.getSource());
        assertEquals(1, searchRecord.getId());
        assertEquals(2f, searchRecord.getScore());
        assertEquals(3, searchRecord.getShardIndex());
        assertEquals(4, searchRecord.getPartitionIndex());

        PersonJava person = searchRecord.getSource();
        assertEquals("Joe", person.getFirstName());
        assertEquals("Duck", person.getLastName());
        assertEquals(32, person.getAge());
    }
}
