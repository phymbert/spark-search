/**
 * Copyright © 2020 Spark Search (The Spark Search Contributors)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.search.rdd

import java.io.{File, FileInputStream, InputStream, OutputStream}
import java.nio.file.{Files, StandardCopyOption}
import java.util.function.Consumer
import java.util.zip.{ZipEntry, ZipInputStream, ZipOutputStream}

private[rdd] object ZipUtils {

  class IteratorInputStream(it: Iterator[Array[Byte]]) extends InputStream {
    var offset: Int = 0
    var buff: Array[Byte] = Array.empty

    override def read(): Int = { // FIXME the slowest implementation ever
      if (offset < buff.length) {
        val b = buff(offset)
        offset = offset + 1
        b & 0xFF // Make sure value is between > 0..255
      } else if (it.hasNext) {
        offset = 1
        buff = it.next
        buff(0) & 0xFF
      } else {
        -1
      }
    }
  }

  class FileInputStreamIterator(filePath: File) extends Iterator[Array[Byte]] {
    override def hasNext: Boolean = {
      if (!finished) {
        if (is == null) {
          is = new FileInputStream(filePath)
        }

        read = is.read(_next)
        finished = read < 0

        if (finished) {
          is.close()
        }
      }
      !finished
    }

    var is: InputStream = _
    var read: Int = -1
    val _next: Array[Byte] = new Array[Byte](8192)
    var finished = false

    override def next(): Array[Byte] = {
      _next.slice(0, read)
    }
  }

  def unzipPartition(indexDir: String, it: Iterator[Array[Byte]]): Unit = {
    unzipPartition(indexDir, new IteratorInputStream(it))
  }

  def unzipPartition(indexDir: String, is: InputStream): Unit = {
    val parentLocalFile = new File(indexDir)
    if (parentLocalFile.mkdirs()) {
      val zis = new ZipInputStream(is)
      Stream.continually(zis.getNextEntry).takeWhile(_ != null).foreach { file =>
        Files.copy(zis, new File(parentLocalFile, file.getName).toPath, StandardCopyOption.REPLACE_EXISTING)
        zis.closeEntry()
      }
      zis.close()
    }
    is.close()
  }

  def zipPartition(localIndexDirPath: java.nio.file.Path, fos: OutputStream): Unit = {
    val zip = new ZipOutputStream(fos)
    val files = Files.list(localIndexDirPath)
    files.forEach {
      new Consumer[java.nio.file.Path] {
        override def accept(file: java.nio.file.Path): Unit = {
          zip.putNextEntry(new ZipEntry(file.toFile.getName))
          Files.copy(file, zip)
          zip.closeEntry()
        }
      }
    }
    files.close()
    zip.close()
    fos.close()
  }
}
