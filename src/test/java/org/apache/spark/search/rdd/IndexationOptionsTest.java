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

import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertEquals;

public class IndexationOptionsTest {
    @Test(expected = IllegalArgumentException.class)
    public void testRequireIndexDirectoryNotNull() {
        try {
            IndexationOptions.builder().rootIndexDirectory(null);
        } catch (IllegalArgumentException e) {
            assertEquals("invalid indexing option argument: root index directory is null", e.getMessage());
            throw e;
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRequireValidMemoryRatio() {
        try {
            IndexationOptions.builder().cacheMemoryExecutorRatio(1);
        } catch (IllegalArgumentException e) {
            assertEquals("invalid indexing option argument: invalid cache memory executor ratio", e.getMessage());
            throw e;
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRequireNotEmpty() {
        try {
            IndexationOptions.builder().notStoredFields(Collections.emptyList());
        } catch (IllegalArgumentException e) {
            assertEquals("invalid indexing option argument: not stored fields", e.getMessage());
            throw e;
        }
    }
}
