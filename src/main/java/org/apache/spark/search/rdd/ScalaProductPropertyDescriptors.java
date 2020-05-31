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

import org.apache.commons.collections.FastHashMap;
import scala.Product;

import java.beans.IntrospectionException;
import java.beans.PropertyDescriptor;
import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.HashMap;
import java.util.Map;

class ScalaProductPropertyDescriptors implements Serializable {

    private static final long serialVersionUID = 1L;

    protected static final String PRODUCT_FIELD_TYPE = "product.field.type";

    private FastHashMap productDescriptors;

    protected PropertyDescriptor[] getProductPropertyDescriptors(scala.Product element) throws IntrospectionException {
        return getProductPropertyDescriptors(element.getClass());
    }

    protected PropertyDescriptor[] getProductPropertyDescriptors(Class<? extends Product> caseClass) throws IntrospectionException {
        if (productDescriptors == null) {
            synchronized (this) {
                if (productDescriptors == null) {
                    productDescriptors = new FastHashMap();
                    productDescriptors.setFast(true);
                }
            }
        }
        PropertyDescriptor[] propertyDescriptors = (PropertyDescriptor[]) productDescriptors.get(caseClass);
        if (propertyDescriptors != null) {
            return propertyDescriptors;
        }

        Map<String, PropertyDescriptor> propertyDescriptorsMap = new HashMap<>();
        int fieldIndex = 0;
        // while scala.Product#productElementNames (scala 1.13) is not shipped to spark (4 ?)
        // https://stackoverflow.com/questions/31189754/get-field-names-list-from-case-class
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
                default:
                    PropertyDescriptor propertyDescriptor = new PropertyDescriptor(methodName, method, null);
                    propertyDescriptor.setValue(PRODUCT_FIELD_TYPE, method.getReturnType());
                    propertyDescriptorsMap.put(methodName, propertyDescriptor);
                    break;
            }
        }

        // Reorder them based on the constructor order
        propertyDescriptors = new PropertyDescriptor[propertyDescriptorsMap.size()];
        Constructor<?> constructor = caseClass.getDeclaredConstructors()[0];
        Parameter[] parameters = constructor.getParameters();
        for (int i = 0; i < parameters.length; i++) {
            Parameter parameter = parameters[i];
            propertyDescriptors[i] = propertyDescriptorsMap.get(parameter.getName());
        }

        productDescriptors.put(caseClass, propertyDescriptors);
        return propertyDescriptors;
    }
}
