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
package org.apache.spark.search.rdd

import java.io.{ByteArrayOutputStream, File, FileInputStream, FileOutputStream}

import org.apache.commons.io.{FileUtils, IOUtils}
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.document.Field.Store
import org.apache.lucene.document.{Document, StringField}
import org.apache.lucene.index.{IndexWriter, IndexWriterConfig}
import org.apache.lucene.store.MMapDirectory
import org.apache.spark.search.rdd.ZipUtils._
import org.scalatest.flatspec.AnyFlatSpec

class ZipUtilsSuite extends AnyFlatSpec {
  it should "create an iterator from a file input stream" in {
    val f = File.createTempFile("test", "test")
    f.deleteOnExit()
    val expected = "abcdef\n" * 100000
    FileUtils.write(f, expected)
    val it = new FileInputStreamIterator(f)
    var actual = ""
    it.foreach { a =>
      actual += new String(a)
    }

    assertResult(expected) {
      actual
    }
  }

  it should "create an input stream from iterator" in {
    val bos = new ByteArrayOutputStream()

    val it = Seq(Seq('a', 'b'), Seq('c', 'd', 'e'), Seq('f'))
      .map(s => s.map(c => c.toByte).toArray)
      .iterator

    IOUtils.copy(new IteratorInputStream(it), bos)

    assertResult("abcdef") {
      new String(bos.toByteArray)
    }
  }

  it should "creates valid zip and able to unzip it" in {
    val testDir = new File(System.getProperty("java.io.tmpdir"), "test-zipping")
    testDir.delete()
    testDir.mkdir()
    val f = new File(testDir, "test")
    f.deleteOnExit()
    val expected = "abcdef\n" * 100000
    FileUtils.write(f, expected)

    val zipped = File.createTempFile("zipped", ".zip")
    f.deleteOnExit()

    zipPartition(testDir.toPath, new FileOutputStream(zipped))
    f.delete()
    testDir.delete()

    unzipPartition(testDir.getAbsolutePath, new FileInputStreamIterator(zipped))
    zipped.delete()

    val bos = new ByteArrayOutputStream()
    IOUtils.copy(new FileInputStream(f), bos)

    assertResult(expected) {
      new String(bos.toByteArray)
    }
  }
}
