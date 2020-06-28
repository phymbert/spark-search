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
package org.apache.spark.search;

import org.apache.lucene.analysis.Analyzer;

import java.io.Serializable;
import java.util.function.Function;

/**
 * Search RDD options.
 */
public class SearchOptions<T> implements Serializable {
    private static final long serialVersionUID = 1L;

    private static final SearchOptions DEFAULT = builder().build();

    private IndexationOptions<T> indexationOptions;

    private ReaderOptions<T> readerOptions;

    // Hidden, use builder or default.
    private SearchOptions() {
    }

    /**
     * @return Default search rdd options.
     */
    public static <T> SearchOptions<T> defaultOptions() {
        return DEFAULT;
    }

    /**
     * Search RDD options builder.
     *
     * @return Search builder
     */
    public static <T> Builder<T> builder() {
        return new Builder<>();
    }

    public IndexationOptions<T> getIndexationOptions() {
        return indexationOptions;
    }

    public ReaderOptions<T> getReaderOptions() {
        return readerOptions;
    }

    /**
     * Indexation option builder.
     */
    public static final class Builder<T> extends SearchBaseOptionsBuilder {
        private final SearchOptions<T> options = new SearchOptions<T>();
        private IndexationOptions.Builder<T> indexationOptionsBuilder = IndexationOptions.builder();
        private ReaderOptions.Builder<T> readerOptionsBuilder = ReaderOptions.builder();

        private Builder() {
        }

        /**
         * Indexations options builder.
         */
        public Builder<T> index(Function<IndexationOptions.Builder<T>, IndexationOptions.Builder<T>> indexationOption) {
            indexationOptionsBuilder = indexationOption.apply(indexationOptionsBuilder);
            return this;
        }

        /**
         * Reader options builder.
         */
        public Builder<T> read(Function<ReaderOptions.Builder<T>, ReaderOptions.Builder<T>> readerOption) {
            readerOptionsBuilder = readerOption.apply(readerOptionsBuilder);
            return this;
        }

        /**
         * Provides the lucene directory where the index will be stored.
         *
         * @param indexDirectoryProvider directory provider.
         * @return builder
         */
        public Builder<T> directoryProvider(IndexDirectoryProvider indexDirectoryProvider) {
            requireNotNull(indexDirectoryProvider, "index directory provider");
            indexationOptionsBuilder.directoryProvider(indexDirectoryProvider);
            readerOptionsBuilder.directoryProvider(indexDirectoryProvider);
            return this;
        }

        /**
         * Common analyzer to use both at indexation and search time.
         */
        public Builder<T> analyzer(Class<? extends Analyzer> analyzer) {
            requireNotNull(analyzer, "analyzer");
            indexationOptionsBuilder.analyzer(analyzer);
            readerOptionsBuilder.analyzer(analyzer);
            return this;
        }

        /**
         * @return built options.
         */
        public SearchOptions<T> build() {
            options.indexationOptions = indexationOptionsBuilder.build();
            options.readerOptions = readerOptionsBuilder.build();
            require(options.indexationOptions.indexDirectoryProvider.getClass()
                            .isAssignableFrom(options.indexationOptions.indexDirectoryProvider.getClass()),
                    "index directory are not compatibles between reader and indexer");
            return options;
        }
    }
}
