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

import org.apache.commons.beanutils.PropertyUtils;
import org.apache.commons.collections.FastHashMap;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import scala.Product;

import java.beans.IntrospectionException;
import java.beans.PropertyDescriptor;
import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.List;

/**
 * Update lucene {@link org.apache.lucene.document.Document}
 * based on the input bean by using reflectivity.
 *
 * @author Pierrick HYMBERT
 */
public class DocumentBeanUpdater<T> implements DocumentUpdater<T>, Serializable {

    private static final long serialVersionUID = 1L;
    private FastHashMap productDescriptors;

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
            propertyDescriptors = describeCaseClass((scala.Product) element);
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

    private PropertyDescriptor[] describeCaseClass(scala.Product element) throws IntrospectionException {
        int fieldSize = element.productArity();
        PropertyDescriptor[] propertyDescriptors = new PropertyDescriptor[fieldSize];
        int fieldIndex = 0;
        // while scala.Product.productElementNames is not shipped
        Class<? extends Product> caseClass = element.getClass();
        for (Method method : caseClass.getDeclaredMethods()) {
            String methodName = method.getName();
            if (method.getParameterCount() > 0) {
                continue;
            }
            if (methodName.contains("$")) {
                continue;
            }
            switch (methodName) {
                case "toString":
                case "hashCode":
                case "productIterator":
                case "productArity":
                case "productElementNames":
                case "productPrefix":
                    break;
                default: // FIXME quite dangerous and will raise IndexOutOfBounds but better than indexing not necessary fields
                    propertyDescriptors[fieldIndex++] = new PropertyDescriptor(methodName, method, null);
                    break;
            }
        }
        if (productDescriptors == null) {
            productDescriptors = new FastHashMap();
            productDescriptors.setFast(true);
        }
        productDescriptors.put(caseClass, propertyDescriptors);
        return propertyDescriptors;
    }


    private PropertyDescriptor getPropertyDescriptor(T element, String name) {
        PropertyDescriptor[] descriptors;
        if (element instanceof scala.Product) {
            descriptors = (PropertyDescriptor[]) productDescriptors.get(element.getClass());
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
        Object value = propertyDescriptor.getReadMethod().invoke(element);

        if (value != null) {
            return value.toString();
        }
        return "";
    }
}
