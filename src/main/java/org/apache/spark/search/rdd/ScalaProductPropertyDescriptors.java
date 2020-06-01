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

import com.google.common.collect.Collections2;
import org.apache.commons.collections.FastHashMap;
import org.apache.commons.lang3.ClassUtils;
import org.apache.spark.search.SearchException;
import org.slf4j.LoggerFactory;
import scala.Product;

import java.beans.IntrospectionException;
import java.beans.PropertyDescriptor;
import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.*;

class ScalaProductPropertyDescriptors implements Serializable {

    private static final long serialVersionUID = 1L;

    protected static final String PRODUCT_FIELD_TYPE = "product.field.type";

    private static final boolean scala211 = scala.util.Properties.versionNumberString().startsWith("2.11");

    static {
        if (scala211) {
            LoggerFactory.getLogger(SearchRDD.class).warn("Scala 2.11 Product constructor parameters are not named, fields order can be randomly switched!");
        }
    }

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

        // while scala.Product#productElementNames (scala 1.13) is not shipped to spark (4 ?)
        // https://stackoverflow.com/questions/31189754/get-field-names-list-from-case-class
        for (Method method : caseClass.getDeclaredMethods()) {
            String methodName = method.getName();
            if (method.getParameterCount() > 0) {
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
                    if (methodName.contains("$")) {
                        break;
                    }
                    PropertyDescriptor propertyDescriptor = new PropertyDescriptor(methodName, method, null);
                    propertyDescriptor.setValue(PRODUCT_FIELD_TYPE, method.getReturnType());
                    propertyDescriptorsMap.put(methodName, propertyDescriptor);
                    break;
            }
        }

        Constructor<?> constructor = caseClass.getDeclaredConstructors()[0];
        if (scala211) {
            // In scala 2.11, constructor parameters are not named
            Class<?>[] constructorParameterTypes = constructor.getParameterTypes();
            Class<?>[] parameterTypes = new Class<?>[propertyDescriptorsMap.size()];
            Collection<List<PropertyDescriptor>> allConstructorParamsPower = Collections2.permutations(propertyDescriptorsMap.values());
            propertyDescriptors = allConstructorParamsPower.stream().peek(s -> {
                Iterator<PropertyDescriptor> it = s.iterator();
                for (int i = 0; i < s.size(); i++) {
                    parameterTypes[i] = (Class<?>) it.next().getValue(PRODUCT_FIELD_TYPE);
                }
            })
                    .filter(pts -> ClassUtils.isAssignable(parameterTypes, constructorParameterTypes, true))
                    .findFirst().orElseThrow(() -> new SearchException("unable to find suitable constructor on " + caseClass))
                    .toArray(new PropertyDescriptor[0]);
        } else {
            // Reorder them based on the constructor order
            propertyDescriptors = new PropertyDescriptor[propertyDescriptorsMap.size()];
            Parameter[] parameters = constructor.getParameters();
            for (int i = 0; i < parameters.length; i++) {
                Parameter parameter = parameters[i];
                propertyDescriptors[i] = propertyDescriptorsMap.get(parameter.getName());
            }
        }

        productDescriptors.put(caseClass, propertyDescriptors);
        return propertyDescriptors;
    }
}
