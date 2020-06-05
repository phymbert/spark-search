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

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.search.*;
import org.apache.spark.search.DocumentConverter;
import org.apache.spark.search.ReaderOptions;
import org.apache.spark.search.SearchException;
import org.apache.spark.search.SearchRecordJava;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Exposes search features on a search partition.
 * <p>
 * Must be executed on the partition executor where the partitions was computed, i.e where the lucene
 * index directory resides/created.
 *
 * @author Pierrick HYMBERT
 */
class SearchPartitionReader<T> implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(SearchPartitionReader.class);

    private final int index;

    private final ReaderOptions<T> options;
    private final IndexSearcher indexSearcher;
    private final String indexDirectory;
    private final AtomicLong queryCount = new AtomicLong();
    private final AtomicLong queryTime = new AtomicLong();
    private final DocumentConverter<T> documentConverter;
    private final DirectoryReader directory;
    private final Class<T> classTag;

    SearchPartitionReader(int index,
                          String indexDirectory,
                          Class<T> classTag,
                          ReaderOptions<T> options)
            throws IOException {
        this.index = index;
        this.indexDirectory = indexDirectory;
        this.classTag = classTag;
        this.options = options;

        this.documentConverter = options.documentConverter;

        this.directory = DirectoryReader.open(options.indexDirectoryProvider.create(Paths.get(indexDirectory)));
        this.indexSearcher = new IndexSearcher(directory);
    }

    Long count() {
        return monitorQuery(() -> (long) indexSearcher.count(new MatchAllDocsQuery()));
    }

    Long count(Query query) {
        return monitorQuery(() -> (long) indexSearcher.count(query));
    }

    SearchRecordJava<T>[] search(Query query, int topK, double minScore) {
        return monitorQuery(() -> {
            TopDocs docs = indexSearcher.search(query, topK);
            return Arrays.stream(docs.scoreDocs)
                    .filter(d -> d.score > minScore)
                    .map(this::convertDoc)
                    .toArray(SearchRecordJava[]::new);
        });
    }

    private SearchRecordJava<T> convertDoc(ScoreDoc scoreDoc) {
        try {
            return documentConverter.convert(index, scoreDoc, classTag, indexSearcher.doc(scoreDoc.doc));
        } catch (Exception e) {
            throw new SearchException("unable to convert scored doc " + scoreDoc.doc + " on partition "
                    + index + " and directory " + indexDirectory, e);
        }
    }

    @Override
    public void close() throws Exception {
        directory.close();
    }

    @FunctionalInterface
    private interface QueryTask<R> {
        R query() throws Exception;
    }

    private <R> R monitorQuery(QueryTask<R> task) {
        try {
            long startTime = System.currentTimeMillis();

            R result = task.query();

            long logQueryTime = options.getLogQueryTime();
            if (logQueryTime > 0) {
                long totalQueryCount = queryCount.incrementAndGet();
                long totalTime = queryTime.addAndGet(System.currentTimeMillis() - startTime);
                if (totalQueryCount % logQueryTime == 0) {
                    logger.info("Queries on partition={}: {}query/s, done={}queries" +
                                    " in={}s  on directory={}",
                            index, (float) totalQueryCount / totalTime * 1000f,
                            totalQueryCount, (float) totalTime / 1000, indexDirectory);
                }
            }
            return result;
        } catch (Exception e) {
            throw new SearchException("query failed on partition "
                    + index + " and directory " + indexDirectory, e);
        }

    }
}
