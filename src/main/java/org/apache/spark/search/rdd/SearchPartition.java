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

import org.apache.commons.io.FileUtils;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.spark.Partition;
import org.apache.spark.search.SearchException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A search RDD partition is associated with a lucene index directory.
 *
 * It is responsible to create the associated Lucene index with the parent partition
 * and run the indexation stage once the the partition is computed.
 * <p>
 * At the moment it cannot be moved across executors and need to be recompute everytime.
 *
 * @author Pierrick HYMBERT
 */
class SearchPartition<T> implements Partition, Serializable {
    private static final long serialVersionUID = 1L;
    private static final Logger logger = LoggerFactory.getLogger(SearchPartition.class);

    final int index;
    final Partition parent;

    /**
     * Lucene index directory on the executor local file system
     */
    final String indexDir;

    SearchPartition(int index, String rootDir, Partition parent) {
        this.index = index;
        this.parent = parent;
        this.indexDir = String.format("%s-index-%d-%d", rootDir, index, System.nanoTime());
    }

    /**
     * Creates a lucene index and starts the indexation.
     *
     * <p>
     * Must be called once the partition is on the target executor.
     *
     * @param elements Elements to index
     * @param options  options of the indexation
     */
    void index(Iterator<T> elements, IndexationOptions<T> options) {
        cleanupDirectoryOnShutdown(options.getIndexDirectoryCleanupHandler());

        monitorIndexation(l -> {
            IndexWriter indexWriter = new IndexWriter(
                    options.getIndexDirectoryProvider().create(Paths.get(indexDir)),
                    new IndexWriterConfig(options.getAnalyzer().newInstance())
                            .setOpenMode(IndexWriterConfig.OpenMode.CREATE)
                            .setCommitOnClose(true)
                            .setUseCompoundFile(false)
                            .setMaxBufferedDocs(Integer.MAX_VALUE)
                            .setRAMBufferSizeMB(Runtime.getRuntime().totalMemory() / 1024. / 1024. * options.getCacheMemoryExecutorRatio())
            );

            // Document will be reuse across elements to save
            // GC cost, updater will just update fields values
            DocumentUpdater.IndexingDocument<T> indexingDocument
                    = new DocumentUpdater.IndexingDocument<>(options);
            elements.forEachRemaining(element -> {
                try {
                    indexingDocument.element = element;
                    options.getDocumentUpdater().update(indexingDocument);
                    indexWriter.addDocument(indexingDocument.doc);
                } catch (Exception e) {
                    throw new SearchException("unable to index document " + element + " got " + e, e);
                }
            });

            // Commit and close the writer
            indexWriter.close();

        }, options.getLogIndexationProgress());
    }

    @FunctionalInterface
    private interface IndexationTask {
        void apply(IndexationListener indexationListener) throws Exception;
    }

    @FunctionalInterface
    private interface IndexationListener {
        void apply();
    }

    private void monitorIndexation(IndexationTask indexationTask, long logIndexationProgress) {
        try {
            logger.info("Starting indexation of partition {} on directory {}", index, indexDir);
            long startTime = System.currentTimeMillis();
            AtomicInteger docCount = new AtomicInteger();

            indexationTask.apply(() -> {
                if (logIndexationProgress > 0) {
                    long currentTotalTime = System.currentTimeMillis() - startTime;
                    long currentDocCount = docCount.incrementAndGet();
                    if (currentDocCount % logIndexationProgress == 0) {
                        logger.debug("Indexing at {}doc/s, done={}",
                                currentDocCount / currentTotalTime * 1000f, currentDocCount);
                    }
                }
            });

            long totalDocCount = docCount.get();
            long totalTime = System.currentTimeMillis() - startTime;
            logger.info("Indexation of partition {} on directory {}: {}doc/s, done={}docs in={}s",
                    index, indexDir, totalDocCount / totalTime * 1000f,
                    totalDocCount, totalTime / 1000);
        } catch (Exception e) {
            throw new SearchException("indexation failed on partition "
                    + index + " and directory " + indexDir, e);
        }

    }

    private void cleanupDirectoryOnShutdown(IndexDirectoryCleanupHandler handler) {
        handler.apply(() -> {
            try {
                FileUtils.deleteDirectory(new File(indexDir));
            } catch (IOException e) {
                logger.warn("unable to cleanup lucene directory of partition {}: {} got {}",
                        index, indexDir, e, e);
            }
        });
    }

    @Override
    public int index() {
        return index;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SearchPartition that = (SearchPartition) o;
        return index == that.index;
    }

    @Override
    public int hashCode() {
        return Integer.hashCode(index);
    }
}
