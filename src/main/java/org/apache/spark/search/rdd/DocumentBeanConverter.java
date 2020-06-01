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
import org.apache.commons.lang.StringUtils;
import org.apache.lucene.document.Document;
import org.apache.lucene.search.ScoreDoc;
import org.apache.spark.search.SearchException;

import java.beans.PropertyDescriptor;
import java.lang.reflect.Array;
import java.util.Arrays;

/**
 * Converts java bean or scala product to a search record.
 *
 * @author Pierrick HYMBERT
 */
public class DocumentBeanConverter<T> extends ScalaProductPropertyDescriptors implements DocumentConverter<T> {

    private static final long serialVersionUID = 1L;

    @Override
    public SearchRecord<T> convert(int partitionIndex, ScoreDoc scoreDoc, Class<T> classTag, Document doc) throws Exception {
        return new SearchRecord<>(scoreDoc.doc, partitionIndex,
                scoreDoc.score, scoreDoc.shardIndex, convert(classTag, doc));
    }

    private T convert(Class<T> classTag, Document doc) throws Exception {
        PropertyDescriptor[] propertyDescriptors;
        boolean scalaProduct = scala.Product.class.isAssignableFrom(classTag);
        if (scalaProduct) {
            propertyDescriptors = getProductPropertyDescriptors((Class) classTag);
        } else {
            propertyDescriptors = PropertyUtils.getPropertyDescriptors(classTag);
        }

        T source;
        if (scalaProduct) {
            Class<?>[] types = new Class[propertyDescriptors.length];
            Object[] values = new Object[propertyDescriptors.length];
            for (int i = 0; i < types.length; i++) {
                PropertyDescriptor propertyDescriptor = propertyDescriptors[i];
                String fieldName = propertyDescriptor.getName();
                String value = doc.get(fieldName);
                types[i] = (Class<?>) propertyDescriptor.getValue(PRODUCT_FIELD_TYPE);
                try {
                    values[i] = ConvertUtils.convert(value, types[i]);
                } catch (Exception e) {
                    throw new SearchException("unable to convert property "
                            + fieldName + " on " + classTag + " from value '" + value + "'", e);
                }
            }
            try {
                source = classTag.getDeclaredConstructor(types).newInstance(values);
            } catch (Exception e) {
                throw new SearchException("unable to invoke case class constructor on "
                        + classTag + " with values '" + Arrays.toString(values) + "'", e);
            }
        } else {
            source = classTag.newInstance();
            for (PropertyDescriptor propertyDescriptor : propertyDescriptors) {
                String fieldName = propertyDescriptor.getName();
                String value = doc.get(fieldName);
                if (StringUtils.isEmpty(value)) {
                    continue;
                }
                try {
                    if (propertyDescriptor.getWriteMethod() != null) {
                        Class<?> parameterType = propertyDescriptor.getWriteMethod().getParameterTypes()[0];
                        Object convertedValue;
                        if (parameterType.isArray()) {
                            // org.apache.commons.beanutils.ConvertUtilsBean.convert(java.lang.Object)
                            // If the specified value is an array, the first element (converted to a String) will be returned
                            parameterType = parameterType.getComponentType();
                            convertedValue = Array.newInstance(parameterType, 1);
                            Array.set(convertedValue, 0, ConvertUtils.convert(value, parameterType));
                        } else {
                            convertedValue = ConvertUtils.convert(value, parameterType);
                        }

                        propertyDescriptor.getWriteMethod().invoke(source, convertedValue);
                    }
                } catch (Exception e) {
                    throw new SearchException("unable to set property '"
                            + fieldName + "' on " + classTag + " from value '" + value + "'", e);
                }
            }
        }
        return source;
    }
}
