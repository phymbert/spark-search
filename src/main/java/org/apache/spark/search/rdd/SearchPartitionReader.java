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
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Paths;

/**
 * Exposes search features on a search partition.
 * <p>
 * Must be executed on the partition executor where the partitions was computed, i.e where the lucene
 * index directory resides/created.
 *
 * @author Pierrick HYMBERT
 */
class SearchPartitionReader<T> {

    private static final Logger logger = LoggerFactory.getLogger(SearchPartitionReader.class);

    private final int index;

    private final ReaderOptions<T> options;
    private final IndexSearcher indexSearcher;
    private final QueryParser queryParser;

    SearchPartitionReader(int index,
                          String indexDirectory,
                          ReaderOptions<T> options)
            throws IOException, IllegalAccessException, InstantiationException {
        this.index = index;
        this.options = options;

        this.queryParser = new QueryParser(options.getDefaultFieldName(),
                options.getAnalyzer().newInstance());
        this.indexSearcher = new IndexSearcher(
                DirectoryReader.open(options.indexDirectoryProvider.create(Paths.get(indexDirectory))));
    }

    Long count() throws IOException {
        return (long) indexSearcher.count(new MatchAllDocsQuery());
    }
}
