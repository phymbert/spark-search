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
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

class ScalaProductPropertyDescriptors implements Serializable {

    private static final long serialVersionUID = 1L;

    protected static final String PRODUCT_FIELD_TYPE = "product.field.type";

    private FastHashMap productDescriptors;

    protected PropertyDescriptor[] getProductPropertyDescriptors(scala.Product element) throws IntrospectionException, NoSuchMethodException {
        return getProductPropertyDescriptors(element.getClass());
    }

    protected PropertyDescriptor[] getProductPropertyDescriptors(Class<? extends Product> caseClass) throws IntrospectionException, NoSuchMethodException {
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

        Field[] fields = caseClass.getDeclaredFields();
        List<PropertyDescriptor> propertyDescriptorsList = new ArrayList<>();
        for (Field field : fields) {
            String fieldName = field.getName();
            if (fieldName.startsWith("$")) {
                continue; // $jacocoData and co
            }
            PropertyDescriptor propertyDescriptor = new PropertyDescriptor(fieldName, caseClass.getDeclaredMethod(fieldName), null);
            propertyDescriptor.setValue(PRODUCT_FIELD_TYPE, field.getType());
            propertyDescriptorsList.add(propertyDescriptor);
        }
        propertyDescriptors = propertyDescriptorsList.toArray(new PropertyDescriptor[propertyDescriptorsList.size()]);

        productDescriptors.put(caseClass, propertyDescriptors);
        return propertyDescriptors;
    }
}
