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

import java.io.ByteArrayInputStream

import org.apache.spark.internal.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.connector.catalog.{CatalogPlugin, Identifier}
import org.apache.spark.sql.execution.datasources.v2.{BatchScanExec, DataSourceRDDPartition}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite

class InMemoryColumnarTableSuite extends AnyFunSuite
  with TestSparkSession
  with BeforeAndAfterEach
  with Logging {

  private def catalog(name: String): CatalogPlugin = {
    spark.sessionState.catalogManager.catalog(name)
  }

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    spark.conf.set("spark.sql.catalog.columnar", classOf[InMemoryTableCatalog].getName)
  }

  override protected def afterEach(): Unit = {
    spark.sessionState.catalogManager.reset()
    spark.sessionState.conf.clear()
    super.afterEach()
    assert(OffHeapAllocator.bufferAllocator.getAllocatedMemory == 0)
  }

  private def createPeopleCSVDf(): DataFrame = {
    spark.sqlContext.read.format("csv")
      .option("header", "false")
      .option("inferSchema", "true")
      .load(peopleCSVLocation)
      .withColumnRenamed("_c0", "name")
      .withColumnRenamed("_c1", "age")
      .withColumnRenamed("_c2", "job")
  }

  test("Save data with partition column") {
    val peopleCSVDf = createPeopleCSVDf()
    peopleCSVDf.createTempView("people_csv")

    val catalogName = "columnar"
    val tableName = "people"
    val columnarTableName = s"$catalogName.$tableName"
    try {
      spark.table("people_csv")
        .write
        .partitionBy("job")
        .saveAsTable(columnarTableName)

      val expectedPartitionCount = peopleCSVDf.select("job").distinct().count()
      val inMemoryTable = catalog(catalogName).asInstanceOf[InMemoryTableCatalog]
        .loadTable(Identifier.of(Array.empty[String], tableName))
        .asInstanceOf[InMemoryColumnarTable]
      assertResult(expectedPartitionCount)(inMemoryTable.dataMap.size)

      val expected = peopleCSVDf.orderBy(col("name"), col("age"))
        .collect()
        .map(row => row.toSeq)
      val actual = spark.table(columnarTableName)
        .orderBy(col("name"), col("age"))
        .collect()
        .map(row => row.toSeq)
      assertResult(expected)(actual)
    } finally {
      spark.sql(s"DROP TABLE $columnarTableName").collect()
      spark.sql("DROP VIEW people_csv").collect()
      assert(OffHeapAllocator.bufferAllocator.getAllocatedMemory == 0)
    }
  }

  test("Save data without partition column") {
    val peopleCSVDf = createPeopleCSVDf()
    peopleCSVDf.createTempView("people_csv")
    val catalogName = "columnar"
    val tableName = "people"
    val columnarTableName = s"$catalogName.$tableName"
    try {
      spark.table("people_csv")
        .write
        .saveAsTable(columnarTableName)

      val inMemoryTable = catalog(catalogName).asInstanceOf[InMemoryTableCatalog]
        .loadTable(Identifier.of(Array.empty[String], tableName))
        .asInstanceOf[InMemoryColumnarTable]
      assertResult(1)(inMemoryTable.dataMap.size)

      val expected = peopleCSVDf.orderBy(col("name"), col("age"))
        .collect()
        .map(row => row.toSeq)
      val actual = spark.table(columnarTableName)
        .orderBy(col("name"), col("age"))
        .collect()
        .map(row => row.toSeq)
      assertResult(expected)(actual)
    } finally {
      spark.sql(s"DROP TABLE $columnarTableName").collect()
      spark.sql("DROP VIEW people_csv").collect()
      assert(OffHeapAllocator.bufferAllocator.getAllocatedMemory == 0)
    }
  }

  test("Drop Table closes ValueVector of Arrow") {
    val df = spark.createDataFrame(Seq((1L, "a"), (2L, "b"), (3L, "c"))).toDF("id", "data")
    df.write.saveAsTable("columnar.test")
    val inMemoryTable = catalog("columnar").asInstanceOf[InMemoryTableCatalog]
      .loadTable(Identifier.of(Array.empty[String], "test"))
      .asInstanceOf[InMemoryColumnarTable]
    assertResult(1)(inMemoryTable.dataMap.size)
    assert(OffHeapAllocator.bufferAllocator.getAllocatedMemory > 0)
    dropTableAndCheckMemoryLeak("columnar", "test")
  }

  test("Insert Data into existing partition") {
    val df = spark.createDataFrame(Seq((1L, "a"), (2L, "b"), (3L, "c"))).toDF("id", "data")
    df.write
      .partitionBy("id")
      .saveAsTable("columnar.test")
    spark.createDataFrame(Seq((3L, "d"))).write
      .insertInto("columnar.test")
    spark.sql("SELECT data FROM columnar.test WHERE id = 3L ORDER BY data").explain(true)

    val result = spark.sql("SELECT data FROM columnar.test WHERE id = 3L ORDER BY data")
      .collect()
    assertResult(Array("c", "d"))(result.map(_.getString(0)))

    assert(OffHeapAllocator.bufferAllocator.getAllocatedMemory > 0)
    dropTableAndCheckMemoryLeak("columnar", "test")
  }

  test("Pushdown projection : Prune columns in DataSource layer") {
    val df = spark.createDataFrame(Seq((1L, "a"), (2L, "b"), (3L, "c"))).toDF("id", "data")
    df.write.saveAsTable("columnar.test")

    val partition = spark.table("columnar.test")
      .select("id")
      .queryExecution.sparkPlan
      .executeColumnar()
      .partitions(0)
      .asInstanceOf[DataSourceRDDPartition]
      .inputPartition
      .asInstanceOf[InMemoryTableScanPartition]

    var columnarBatch: ColumnarBatch = null
    try {
       columnarBatch = ArrowAdapter.deserializeRecordBatchInputStream(
        ArrowAdapter.deserializeArrowSchemaBinary(partition.arrowSchema),
        new ByteArrayInputStream(partition.recordBatch),
        OffHeapAllocator.bufferAllocator
      )
      assertResult(3)(columnarBatch.numRows())
      assertResult(1)(columnarBatch.numCols())
    } finally {
      columnarBatch.close()
    }
    dropTableAndCheckMemoryLeak("columnar", "test")
  }

  test("Pushdown projection : no column is requested.") {
    val df = spark.createDataFrame(Seq((1L, "a"), (2L, "b"), (3L, "c"))).toDF("id", "data")
    df.write.saveAsTable("columnar.test")
    val partition = spark.sql("SELECT COUNT(1) FROM columnar.test")
      .queryExecution.sparkPlan
      .find(_.isInstanceOf[BatchScanExec])
      .get
      .executeColumnar()
      .partitions(0)
      .asInstanceOf[DataSourceRDDPartition]
      .inputPartition
      .asInstanceOf[InMemoryTableScanPartition]

    var columnarBatch: ColumnarBatch = null
    try {
      columnarBatch = ArrowAdapter.deserializeRecordBatchInputStream(
        ArrowAdapter.deserializeArrowSchemaBinary(partition.arrowSchema),
        new ByteArrayInputStream(partition.recordBatch),
        OffHeapAllocator.bufferAllocator
      )
      assertResult(3)(columnarBatch.numRows())
      assertResult(0)(columnarBatch.numCols())
    } finally {
      columnarBatch.close()
    }
    dropTableAndCheckMemoryLeak("columnar", "test")
  }

  private def dropTableAndCheckMemoryLeak(catalogName: String, tableName: String): Unit = {
    catalog(catalogName).asInstanceOf[InMemoryTableCatalog].dropTable(
      Identifier.of(Array.empty[String], tableName))
    assert(OffHeapAllocator.bufferAllocator.getAllocatedMemory == 0)
  }
}
