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
package org.apache.spark.search.reflect;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.spark.search.DocumentUpdater;

import java.beans.IntrospectionException;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Method;
import java.util.List;

/**
 * Update lucene {@link org.apache.lucene.document.Document}
 * based on the input bean by using reflectivity.
 *
 * @author Pierrick HYMBERT
 */
public class DocumentBeanUpdater<T> extends DocumentBasePropertyDescriptors implements DocumentUpdater<T> {

    private static final long serialVersionUID = 1L;

    @Override
    public void update(IndexingDocument<T> indexingDocument) throws Exception {
        if (indexingDocument.doc.getFields().isEmpty()) {
            buildFields(indexingDocument);
        }

        T element = indexingDocument.element;
        List<IndexableField> fields = indexingDocument.doc.getFields();
        for (IndexableField field : fields) {
            PropertyDescriptor propertyDescriptor = getPropertyDescriptor(element, field.name());
            // In this first version of spark-search, everything is stored as string
            // since spark as built-in filtering/explode feature. We do not need lucene
            // for that purpose. Indeed the developer can join the result after the search stage done.
            // For arrays string search, the input RDD might be exploded
            // FIXME GitHub Issue #21 support array
            ((Field) field).setStringValue(value(propertyDescriptor, element));
        }
    }

    private void buildFields(IndexingDocument<T> indexingDocument) throws Exception {
        T element = indexingDocument.element;
        PropertyDescriptor[] propertyDescriptors = getPropertyDescriptors(element.getClass());
        for (PropertyDescriptor propertyDescriptor : propertyDescriptors) {
            Method readMethod = propertyDescriptor.getReadMethod();
            if (readMethod == null) {
                continue;
            }
            String fieldName = propertyDescriptor.getName();
            if ("class".equals(fieldName)) {
                continue;
            }
            if (!readMethod.isAccessible()) {
                readMethod.setAccessible(true);
            }
            Field.Store storedField = Field.Store.YES;
            if (!indexingDocument.options.isStoreFields()
                    || indexingDocument.options.getNotStoredFields().contains(fieldName)) {
                storedField = Field.Store.NO;
            }

            Field field;
            if (indexingDocument.options.getFieldIndexOptions() != IndexOptions.DOCS) {
                FieldType fieldType = new FieldType();
                fieldType.setIndexOptions(indexingDocument.options.getFieldIndexOptions());
                if (storedField == Field.Store.YES) {
                    fieldType.setStored(true);
                }
                fieldType.freeze();
                field = new Field(fieldName, "", fieldType);
            } else {
                field = new StringField(fieldName, "", storedField);
            }
            indexingDocument.doc.add(field);
        }
    }

    private PropertyDescriptor getPropertyDescriptor(T element, String name) throws IntrospectionException, NoSuchMethodException {
        PropertyDescriptor[] descriptors = getPropertyDescriptors(element.getClass());
        for (PropertyDescriptor descriptor : descriptors) {
            if (name.equals(descriptor.getName()))
                return descriptor;
        }
        throw new IllegalStateException("no property descriptors for field " + name + " on " + element.getClass());
    }
}
