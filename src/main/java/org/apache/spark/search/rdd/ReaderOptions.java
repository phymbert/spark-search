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

import org.apache.lucene.analysis.Analyzer;

import java.io.Serializable;

/**
 * Search reader options.
 */
public class ReaderOptions<T> implements Serializable {
    private static final long serialVersionUID = 1L;

    private static final ReaderOptions DEFAULT = builder().build();

    /**
     * Default field name for query terms.
     */
    public static final String DEFAULT_FIELD_NAME = "__default__";
    private String defaultFieldName = DEFAULT_FIELD_NAME;

    /**
     * Default search analyzer type: standard.
     */
    public Class<? extends Analyzer> analyzer = IndexationOptions.DEFAULT_ANALYZER;

    /**
     * Directory is {@link org.apache.lucene.store.MMapDirectory} by default.
     */
    IndexDirectoryProvider indexDirectoryProvider = IndexationOptions.DEFAULT_DIRECTORY_PROVIDER;

    /**
     * Default document converter.
     */
    Class<? extends DocumentConverter<T>> documentConverter = (Class) DocumentBeanConverter.class;

    /**
     * Log query time every 10K queries.
     */
    public static final long DEFAULT_LOG_QUERY_TIME = -1;
    private long logQueryTime = DEFAULT_LOG_QUERY_TIME;

    // Hidden, use builder or default.
    private ReaderOptions() {
    }

    /**
     * @return Default search reader options.
     */
    public static <T> ReaderOptions<T> defaultOptions() {
        return DEFAULT;
    }

    /**
     * Search reader options builder.
     *
     * @return Search reader builder
     */
    public static <T> Builder<T> builder() {
        return new Builder<>();
    }

    public String getDefaultFieldName() {
        return defaultFieldName;
    }

    public Class<? extends Analyzer> getAnalyzer() {
        return analyzer;
    }

    IndexDirectoryProvider getIndexDirectoryProvider() {
        return indexDirectoryProvider;
    }

    long getLogQueryTime() {
        return logQueryTime;
    }

    Class<? extends DocumentConverter<T>> getDocumentConverter() {
        return documentConverter;
    }

    /**
     * Indexation option builder.
     */
    public static final class Builder<T> extends SearchBaseOptionsBuilder {
        private final ReaderOptions<T> options = new ReaderOptions<T>();

        private Builder() {
        }

        /**
         * Field search analyzer.
         *
         * @param analyzer search analyzer
         * @return builder
         */
        public Builder<T> analyzer(Class<? extends Analyzer> analyzer) {
            requireNotNull(analyzer, "analyzer");
            options.analyzer = analyzer;
            return this;
        }

        /**
         * Default field name for query terms.
         *
         * @param defaultFieldName default field name
         * @return builder
         */
        public Builder<T> defaultFieldName(String defaultFieldName) {
            requireNotNull(defaultFieldName, "default field name");
            options.defaultFieldName = defaultFieldName;
            return this;
        }

        /**
         * Provides the lucene directory where the index is stored.
         * Must be the same directory used at indexation time.
         *
         * @param indexDirectoryProvider directory provider.
         * @return builder
         */
        public Builder<T> directoryProvider(IndexDirectoryProvider indexDirectoryProvider) {
            requireNotNull(indexDirectoryProvider, "index directory provider");
            options.indexDirectoryProvider = indexDirectoryProvider;
            return this;
        }

        /**
         * Log the query time every X queries on the same partition operation.
         * <p>
         * Set 0 or negative value to disable logging.
         *
         * @param logQueryTime Modulo of document queries to log performance
         * @return builder
         */
        public Builder<T> logQueryTime(long logQueryTime) {
            options.logQueryTime = logQueryTime;
            return this;
        }

        /**
         * Convert the scored document and lucene document to target type.
         *
         * @param documentConverter Document converter
         * @return builder
         */
        public Builder<T> documentConverter(Class<? extends DocumentConverter<T>> documentConverter) {
            requireNotNull(documentConverter, "document converter");
            options.documentConverter = documentConverter;
            return this;
        }

        /**
         * @return built options.
         */
        public ReaderOptions<T> build() {
            return options;
        }
    }
}
