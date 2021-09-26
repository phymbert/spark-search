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
package org.apache.spark.sql.search

import io.netty.util.internal.logging.{InternalLoggerFactory, Slf4JLoggerFactory}
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Suite}

trait LocalSparkSession extends BeforeAndAfterEach with BeforeAndAfterAll {
  self: Suite =>

  @transient var _spark: SparkSession = _

  override def beforeAll() {
    super.beforeAll()
    InternalLoggerFactory.setDefaultFactory(Slf4JLoggerFactory.INSTANCE)
  }

  override def afterEach() {
    try {
      resetSparkSession()
    } finally {
      super.afterEach()
    }
  }

  def resetSparkSession(): Unit = {
    LocalSparkSession.stop(_spark)
    _spark = null
  }

  override def beforeEach() {
    _spark = SparkSession.builder()
      .master("local[2]")
      .appName("Spark SQL Search Test")
      .getOrCreate()
  }
}

object LocalSparkSession {
  def stop(sc: SparkSession) {
    if (sc != null) {
      sc.stop()
    }
    // To avoid RPC rebinding to the same port, since it doesn't unbind immediately on shutdown
    System.clearProperty("spark.driver.port")
  }

  /** Runs `f` by passing in `sc` and ensures that `sc` is stopped. */
  def withSpark[T](sc: SparkSession)(f: SparkSession => T): T = {
    try {
      f(sc)
    } finally {
      stop(sc)
    }
  }

}