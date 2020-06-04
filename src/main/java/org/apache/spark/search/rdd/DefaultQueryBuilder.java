package org.apache.spark.search.rdd;

import org.apache.lucene.queryparser.flexible.core.builders.QueryBuilder;
import org.apache.lucene.search.Query;

import java.util.function.BiFunction;

/**
 * Build a OR query on all fields.
 *
 * @author Pierrick HYMBERT
 */
public class DefaultQueryBuilder<T>
        extends ScalaProductPropertyDescriptors
        implements BiFunction<T, QueryBuilder, Query> {

    private final Class<? extends T> clazz;

    public DefaultQueryBuilder(Class<? extends T> clazz) {
        this.clazz = clazz;
    }

    @Override
    public Query apply(T t, QueryBuilder queryBuilder) {
        return null;
    }
}
