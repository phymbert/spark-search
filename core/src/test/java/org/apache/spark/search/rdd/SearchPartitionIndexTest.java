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
package org.apache.spark.search.rdd;

import org.apache.commons.io.FileUtils;
import org.apache.spark.search.IndexDirectoryCleanupHandler;
import org.apache.spark.search.IndexationOptions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.*;

public class SearchPartitionIndexTest {


    private Runnable cleanupIndexDirectory;
    IndexDirectoryCleanupHandler handler = (cleanupIndexDirectory -> this.cleanupIndexDirectory = cleanupIndexDirectory);

    @BeforeEach
    public void setUp() throws IOException {
        Path rootDir = Paths.get(IndexationOptions.defaultOptions().getRootIndexDirectory());
        if (rootDir.toFile().exists())
            FileUtils.deleteDirectory(rootDir.toFile().getAbsoluteFile());
    }

    @AfterEach
    public void cleanup() {
        cleanupIndexDirectory.run();
        File indexDir = new File(IndexationOptions.defaultOptions().getRootIndexDirectory());
        File[] files = indexDir.listFiles();
        assertNull(files);
    }

    @Test
    public void shouldCreateALuceneIndex() {
        IndexationOptions<PersonJava> options = IndexationOptions.<PersonJava>builder()
                .indexDirectoryCleanupHandler(handler)
                .build();
        SearchPartitionIndex<PersonJava> partition = new SearchPartitionIndex<>(0, options.getRootIndexDirectory(), null,null);
        partition.index(PersonJava.PERSONS.iterator(), options);

        File indexDir = new File(partition.indexDir);
        File[] files = indexDir.listFiles();
        assertNotNull(files);
        assertTrue(files.length > 0);
    }
}
