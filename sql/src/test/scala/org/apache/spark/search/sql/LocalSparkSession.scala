package org.apache.spark.search.sql

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
      .master("local[*]")
      .appName("Spark Search Test")
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