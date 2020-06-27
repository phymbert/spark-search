package org.apache.spark.search.reflect;

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.QueryBuilder;
import org.apache.spark.search.SearchException;
import scala.Serializable;

import java.beans.PropertyDescriptor;
import java.util.function.BiFunction;

/**
 * Build a OR query on all fields.
 *
 * @author Pierrick HYMBERT
 */
public class DefaultQueryBuilder<T>
        extends scala.runtime.AbstractFunction2<T, QueryBuilder, Query>
        implements BiFunction<T, QueryBuilder, Query>, Serializable {

    private static final long serialVersionUID = 1L;

    private final DocumentBasePropertyDescriptors basePropertyDescriptors = new DocumentBasePropertyDescriptors();

    private final Class<? extends T> clazz;

    public DefaultQueryBuilder(Class<? extends T> clazz) {
        this.clazz = clazz;
    }

    @Override
    public Query apply(T element, QueryBuilder queryBuilder) {
        try {
            BooleanQuery.Builder builder = new BooleanQuery.Builder();
            PropertyDescriptor[] descriptors = basePropertyDescriptors.getPropertyDescriptors(element.getClass());
            for (PropertyDescriptor propertyDescriptor : descriptors) {
                // Value cannot be null from above method
                String value = basePropertyDescriptors.value(propertyDescriptor, element);
                if (!"".equals(value)) {
                    builder.add(queryBuilder.createBooleanQuery(propertyDescriptor.getName(), value),
                            BooleanClause.Occur.SHOULD);
                }
            }
            builder.setMinimumNumberShouldMatch(1);
            return builder.build();
        } catch (Exception e) {
            throw new SearchException("unable to build query based on " + element, e);
        }
    }
}
