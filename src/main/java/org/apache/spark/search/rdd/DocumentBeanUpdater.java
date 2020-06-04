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

import org.apache.commons.beanutils.ConvertUtils;
import org.apache.commons.beanutils.PropertyUtils;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;

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
public class DocumentBeanUpdater<T> extends ScalaProductPropertyDescriptors implements DocumentUpdater<T> {

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
            ((Field) field).setStringValue(value(propertyDescriptor, element));
        }
    }

    private void buildFields(IndexingDocument<T> indexingDocument) throws Exception {
        T element = indexingDocument.element;
        PropertyDescriptor[] propertyDescriptors;
        if (element instanceof scala.Product) {
            propertyDescriptors = getProductPropertyDescriptors((scala.Product) element);
        } else {
            propertyDescriptors = PropertyUtils.getPropertyDescriptors(element);
        }
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
        PropertyDescriptor[] descriptors;
        if (element instanceof scala.Product) {
            descriptors = getProductPropertyDescriptors((scala.Product) element);
        } else {
            descriptors = PropertyUtils.getPropertyDescriptors(element);
        }
        for (PropertyDescriptor descriptor : descriptors) {
            if (name.equals(descriptor.getName()))
                return descriptor;
        }
        throw new IllegalStateException("no property descriptors for field " + name + " on " + element.getClass());
    }

    private String value(PropertyDescriptor propertyDescriptor, T element) throws Exception {
        String value = ConvertUtils.convert(propertyDescriptor.getReadMethod().invoke(element));

        if (value != null) {
            return value;
        }
        return "";
    }
}
