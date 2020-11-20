/*
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

package org.apache.spark.sql.connector

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, Suite}

trait TestSparkSession extends BeforeAndAfterAll { self: Suite =>
  protected var spark: SparkSession = _

  lazy val resourceFolder: String = System.getProperty("user.dir") + "/src/test/resources/"

  lazy val peopleCSVLocation: String = resourceFolder + "people.csv"

  override protected def beforeAll(): Unit = {
    spark = SparkSession.builder().appName("r2-test")
      .config("spark.default.parallelism", 10)
      .config("spark.driver.bindAddress", "localhost")
      .master("local[*]")
      .getOrCreate()
  }

  override protected def afterAll(): Unit = {
    spark.stop()
  }
}

