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

package org.apache.spark.search.rdd

import io.netty.util.internal.logging.{InternalLoggerFactory, Slf4JLoggerFactory}
import org.apache.spark.SparkContext
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Suite}

trait LocalSparkContext extends BeforeAndAfterEach with BeforeAndAfterAll {
  self: Suite =>

  @transient var sc: SparkContext = _

  override def beforeAll() {
    super.beforeAll()
    InternalLoggerFactory.setDefaultFactory(Slf4JLoggerFactory.INSTANCE)
  }

  override def afterEach() {
    try {
      resetSparkContext()
    } finally {
      super.afterEach()
    }
  }

  def resetSparkContext(): Unit = {
    LocalSparkContext.stop(sc)
    sc = null
  }

  override def beforeEach(): Unit = {
    sc = new SparkContext("local[2]", "test")
  }

}

object LocalSparkContext {
  def stop(sc: SparkContext) {
    if (sc != null) {
      sc.stop()
    }
    // To avoid RPC rebinding to the same port, since it doesn't unbind immediately on shutdown
    System.clearProperty("spark.driver.port")
  }

  /** Runs `f` by passing in `sc` and ensures that `sc` is stopped. */
  def withSpark[T](sc: SparkContext)(f: SparkContext => T): T = {
    try {
      f(sc)
    } finally {
      stop(sc)
    }
  }

}