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

import org.apache.lucene.document.Document;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.spark.search.DocumentUpdater;
import org.apache.spark.search.IndexationOptions;
import org.apache.spark.search.reflect.DocumentBeanUpdater;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Date;

import static org.junit.jupiter.api.Assertions.*;

public class DocumentBeanUpdaterScalaTest {

    @Test
    public void shouldUpdateDocumentWithAJavaBeanToDocument() throws Exception {
        DocumentBeanUpdater<PersonJava> documentBeanUpdater = new DocumentBeanUpdater<>();
        DocumentUpdater.IndexingDocument<PersonJava> indexingDocument
                = new DocumentUpdater.IndexingDocument<>(IndexationOptions.defaultOptions());
        Document doc = indexingDocument.doc;

        // First element
        indexingDocument.element = new PersonJava(
                "John", "Doe", 34, new Date(1986, 5, 30),
                new Address("10 chemin de la fontaine", "46140".getBytes(), "FR"),
                Collections.emptyList());

        documentBeanUpdater.update(indexingDocument);

        assertEquals(5, doc.getFields().size());
        assertEquals("John", doc.get("firstName"));
        assertNull(doc.get("birthDate")); // No Getter
        assertEquals("34", doc.get("age"));
        assertFalse(doc.get("address").isEmpty());
        // FIXME GitHub Issue #21 support array depends of version of hadoop/commons-beanutils
        assertTrue(Arrays.asList("[]", "").contains(doc.get("friends")));
        assertTrue(doc.getField("age").fieldType().stored());
        assertEquals(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS, doc.getField("lastName").fieldType().indexOptions());

        // Second element
        indexingDocument.element = new PersonJava(
                "Jack", "Daniels", 4, new Date(2015, 5, 30),
                new Address("47 Rue de Monceau 75008 Paris", "75008".getBytes(), "FR"),
                Collections.singletonList(indexingDocument.element));

        documentBeanUpdater.update(indexingDocument);
        assertEquals("Jack", doc.get("firstName"));
        assertEquals("4", doc.get("age"));
        assertNotEquals("[]", doc.get("friends"));
    }

    @Test
    public void shouldNotStoreExcludedFieldDocument() throws Exception {
        DocumentBeanUpdater<PersonJava> documentBeanUpdater = new DocumentBeanUpdater<>();
        DocumentUpdater.IndexingDocument<PersonJava> indexingDocument
                = new DocumentUpdater.IndexingDocument<>(IndexationOptions.<PersonJava>builder()
                .notStoredFields(Collections.singletonList("firstName"))
                .build());
        Document doc = indexingDocument.doc;

        indexingDocument.element = new PersonJava(
                "John", "Doe", 34, new Date(1986, 5, 30),
                new Address("10 chemin de la fontaine", "46140".getBytes(), "FR"),
                Collections.emptyList());

        documentBeanUpdater.update(indexingDocument);
        assertFalse(doc.getField("firstName").fieldType().stored());
    }

    @Test
    public void shouldUseCustomFieldIndexOptions() throws Exception {
        DocumentBeanUpdater<PersonJava> documentBeanUpdater = new DocumentBeanUpdater<>();
        DocumentUpdater.IndexingDocument<PersonJava> indexingDocument
                = new DocumentUpdater.IndexingDocument<>(IndexationOptions.<PersonJava>builder()
                .fieldIndexOptions(IndexOptions.DOCS)
                .build());
        Document doc = indexingDocument.doc;

        indexingDocument.element = new PersonJava();

        documentBeanUpdater.update(indexingDocument);
        IndexableField f = doc.getField("firstName");
        assertEquals(IndexOptions.DOCS, f.fieldType().indexOptions());
        assertEquals(StringField.class, f.getClass());
    }

    @Test
    public void shouldNotStoreField() throws Exception {
        DocumentBeanUpdater<PersonJava> documentBeanUpdater = new DocumentBeanUpdater<>();
        DocumentUpdater.IndexingDocument<PersonJava> indexingDocument
                = new DocumentUpdater.IndexingDocument<>(IndexationOptions.<PersonJava>builder()
                .storeFields(false)
                .build());
        Document doc = indexingDocument.doc;

        indexingDocument.element = new PersonJava();

        documentBeanUpdater.update(indexingDocument);
        IndexableField f = doc.getField("age");
        assertFalse(f.fieldType().stored());
    }
}
