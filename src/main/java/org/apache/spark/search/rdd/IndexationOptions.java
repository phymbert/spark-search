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
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.NoLockFactory;
import org.apache.spark.util.ShutdownHookManager;
import scala.runtime.AbstractFunction0;
import scala.runtime.BoxedUnit;

import java.io.File;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Lucene indexation options.
 */
public final class IndexationOptions<T> implements Serializable {
    private static final long serialVersionUID = 1L;

    /**
     * Default index analyzer type: standard.
     */
    public static final Class<? extends Analyzer> DEFAULT_ANALYZER = StandardAnalyzer.class;
    Class<? extends Analyzer> analyzer = DEFAULT_ANALYZER;

    /**
     * Fields are stored by default in the lucene index.
     */
    public static final boolean DEFAULT_STORE_FIELDS = true;
    private boolean storeFields = DEFAULT_STORE_FIELDS;

    /**
     * Update the lucene document with the next element to be indexed using bean utils by default.
     */
    private DocumentUpdater<T> documentUpdater = new DocumentBeanUpdater<>();

    /**
     * Ratio of the total executor memory used to cache document during indexation.
     */
    public static final double DEFAULT_MEMORY_EXECUTOR_RATIO = 0.4;
    private double cacheMemoryExecutorRatio = DEFAULT_MEMORY_EXECUTOR_RATIO;

    /**
     * Default fields index options: {@link IndexOptions}.
     */
    public static final IndexOptions DEFAULT_FIELD_INDEX_OPTIONS = IndexOptions.DOCS_AND_FREQS_AND_POSITIONS;
    private IndexOptions fieldIndexOptions = DEFAULT_FIELD_INDEX_OPTIONS;

    /**
     * List of field to do not store.
     */
    private List<String> notStoredFields = Collections.emptyList();

    /**
     * Root index directory.
     */
    private String rootIndexDirectory = Optional.ofNullable(System.getProperty(CONF_SPARK_LOCAL_DIR))
            .orElse(System.getProperty(PROPS_JAVA_IO_TMPDIR)) + File.separator + "spark-search"; // file.separator not necessary ?
    private static final String CONF_SPARK_LOCAL_DIR = "spark.local.dir";
    private static final String PROPS_JAVA_IO_TMPDIR = "java.io.tmpdir";

    /**
     * Log indexation progress every 100K docs.
     */
    public static final long DEFAULT_LOG_INDEXATION_PROGRESS = -1;
    private long logIndexationProgress = DEFAULT_LOG_INDEXATION_PROGRESS;

    /**
     * Directory is {@link org.apache.lucene.store.MMapDirectory} by default.
     */
    public static final IndexDirectoryProvider DEFAULT_DIRECTORY_PROVIDER = (indexDir) -> new MMapDirectory(indexDir, NoLockFactory.INSTANCE);
    IndexDirectoryProvider indexDirectoryProvider = DEFAULT_DIRECTORY_PROVIDER;

    /**
     * Lucene indexation directory is deleted by default when the spark context is done.
     */
    public static final IndexDirectoryCleanupHandler DEFAULT_SHUTDOWN_HOOK_HANDLER = (cleanup) -> ShutdownHookManager.addShutdownHook(new AbstractFunction0<BoxedUnit>() {
        @Override
        public BoxedUnit apply() {
            cleanup.run();
            return BoxedUnit.UNIT;
        }
    });
    private IndexDirectoryCleanupHandler indexDirectoryCleanupHandler = DEFAULT_SHUTDOWN_HOOK_HANDLER;

    private static final IndexationOptions DEFAULT = builder().build();

    // Hidden, use builder or default.
    private IndexationOptions() {
    }

    /**
     * @return Default indexation options.
     */
    public static <T> IndexationOptions<T> defaultOptions() {
        return DEFAULT;
    }

    /**
     * Indexation options builder.
     *
     * @return Indexation builder
     */
    public static <T> Builder<T> builder() {
        return new Builder<>();
    }

    Class<? extends Analyzer> getAnalyzer() {
        return analyzer;
    }

    boolean isStoreFields() {
        return storeFields;
    }

    DocumentUpdater<T> getDocumentUpdater() {
        return documentUpdater;
    }

    double getCacheMemoryExecutorRatio() {
        return cacheMemoryExecutorRatio;
    }

    String getRootIndexDirectory() {
        return rootIndexDirectory;
    }

    long getLogIndexationProgress() {
        return logIndexationProgress;
    }

    IndexDirectoryProvider getIndexDirectoryProvider() {
        return indexDirectoryProvider;
    }

    IndexDirectoryCleanupHandler getIndexDirectoryCleanupHandler() {
        return indexDirectoryCleanupHandler;
    }

    List<String> getNotStoredFields() {
        return notStoredFields;
    }

    IndexOptions getFieldIndexOptions() {
        return fieldIndexOptions;
    }

    /**
     * Indexation option builder.
     */
    public static final class Builder<T> extends SearchBaseOptionsBuilder {
        private final IndexationOptions<T> options = new IndexationOptions<T>();

        private Builder() {
        }

        /**
         * Field index analyzer.
         *
         * @param analyzer index analyzer
         * @return builder
         */
        public Builder<T> analyzer(Class<? extends Analyzer> analyzer) {
            requireNotNull(analyzer, "analyzer");
            options.analyzer = analyzer;
            return this;
        }

        /**
         * True to store the field value within lucene,
         * if false fields will not be retrieved and only docid will be available.
         *
         * @param storeFields to store the field in lucene index.
         * @return builder
         */
        public Builder<T> storeFields(boolean storeFields) {
            options.storeFields = storeFields;
            return this;
        }

        /**
         * Update the lucene document with the next element to be indexed.
         *
         * @param documentUpdater updater
         * @return builder
         */
        public Builder<T> documentUpdater(DocumentUpdater<T> documentUpdater) {
            requireNotNull(documentUpdater, "document updater");
            options.documentUpdater = documentUpdater;
            return this;
        }

        /**
         * Ratio of the total executor memory used to cache document during indexation.
         *
         * @param cacheMemoryExecutorRatio ratio of total heap to assign to lucene cache during indexation.
         * @return builder
         */
        public Builder<T> cacheMemoryExecutorRatio(double cacheMemoryExecutorRatio) {
            require(cacheMemoryExecutorRatio > 0 && cacheMemoryExecutorRatio < 1., "invalid cache memory executor ratio");
            options.cacheMemoryExecutorRatio = cacheMemoryExecutorRatio;
            return this;
        }

        /**
         * Provides the lucene directory where the index will be stored.
         * The same directory has to be use at query time.
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
         * Root directory on the executor nodes where indexes will be created.
         * It must be a fast disk on the local machine.
         * <p>
         * Default to spark.local.dir or java.io.tmpdir.
         *
         * @param rootIndexDirectory Root index directory of the lucene index.
         * @return builder
         */
        public Builder<T> rootIndexDirectory(String rootIndexDirectory) {
            requireNotNull(rootIndexDirectory, "root index directory");
            options.rootIndexDirectory = rootIndexDirectory;
            return this;
        }

        /**
         * Log the indexation progress each time this volume of docs has been indexed.
         * <p>
         * Set 0 or negative value to disable indexation progress.
         *
         * @param logIndexationProgress Modulo of document indexed log progress
         * @return builder
         */
        public Builder<T> logIndexationProgress(long logIndexationProgress) {
            options.logIndexationProgress = logIndexationProgress;
            return this;
        }

        /**
         * Handler which will call the cleanup method of each partition when the index has to be deleted.
         *
         * @param indexDirectoryCleanupHandler Cleanup handler
         * @return builder
         */
        public Builder<T> indexDirectoryCleanupHandler(IndexDirectoryCleanupHandler indexDirectoryCleanupHandler) {
            requireNotNull(indexDirectoryCleanupHandler, "index directory cleanup handler");
            options.indexDirectoryCleanupHandler = indexDirectoryCleanupHandler;
            return this;
        }

        /**
         * Indicates that all these field names must not be stored in the index.
         * Note they are still queryable.
         *
         * @param notStoredFields field names excluded from storage in the lucene index
         * @return builder
         */
        public Builder<T> notStoredFields(List<String> notStoredFields) {
            requireNotEmpty(notStoredFields, "not stored fields");
            options.notStoredFields = notStoredFields;
            return this;
        }

        /**
         * Field index options for all indexed fields.
         *
         * @param fieldIndexOptions field index option
         * @return builder
         */
        public Builder<T> fieldIndexOptions(IndexOptions fieldIndexOptions) {
            requireNotNull(fieldIndexOptions, "field index options");
            options.fieldIndexOptions = fieldIndexOptions;
            return this;
        }

        /**
         * @return built options.
         */
        public IndexationOptions<T> build() {
            return options;
        }
    }
}
