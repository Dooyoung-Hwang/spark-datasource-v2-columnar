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

import org.apache.arrow.vector.{Float4Vector, IntVector, VarCharVector}
import org.apache.arrow.vector.complex.{ListVector, StructVector}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.internal.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.connector.catalog.{CatalogPlugin, Identifier}
import org.apache.spark.sql.execution.datasources.v2.{BatchScanExec, DataSourceRDDPartition}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, FloatType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.unsafe.types.UTF8String

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
    // No partition
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
    val inMemoryTable = catalog("columnar").asInstanceOf[InMemoryTableCatalog]
      .loadTable(Identifier.of(Array.empty[String], "test"))
      .asInstanceOf[InMemoryColumnarTable]
    // 3 partitions
    assertResult(3)(inMemoryTable.dataMap.size)

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

  test("Pushdown projection : nested & repeated types") {
    val schema = StructType(Seq(StructField("num", IntegerType, nullable = true),
      StructField("str", StringType, nullable = true),
      StructField("arr", ArrayType(FloatType), nullable = true),
      StructField("repeated",
        ArrayType(
          StructType(Seq(
            StructField("num", IntegerType, nullable = true),
            StructField("str", StringType, nullable = true)
          ))
        )
      )
    ))
    val row = new GenericInternalRow(Array(1, UTF8String.fromString("abc"),
      ArrayData.toArrayData(Array(1.0f, 2.0f)),
      ArrayData.toArrayData(Array(
        new GenericInternalRow(Array(10, UTF8String.fromString("x"))),
        new GenericInternalRow(Array(20, UTF8String.fromString("y")))
      ))
    ))
    val columnarBatch = new WritableColumnarBatch(schema, null)
    try {
      columnarBatch.appendRows(IndexedSeq(row))
      var prunedVector = columnarBatch.pruneFieldVectors(StructType(
        Seq(schema.apply("str"), schema.apply("arr"))))
      assertResult(2)(prunedVector.size())
      assert(prunedVector.get(0).isInstanceOf[VarCharVector])
      assert(prunedVector.get(1).isInstanceOf[ListVector])
      var dataVector = prunedVector.get(1).asInstanceOf[ListVector].getDataVector
      assert(dataVector.isInstanceOf[Float4Vector])

      prunedVector = columnarBatch.pruneFieldVectors(StructType(Seq(schema.apply("repeated"))))
      assertResult(1)(prunedVector.size())
      assert(prunedVector.get(0).isInstanceOf[ListVector])
      dataVector = prunedVector.get(0).asInstanceOf[ListVector].getDataVector
      assert(dataVector.isInstanceOf[StructVector])
      assert(dataVector.asInstanceOf[StructVector].getChild("num").isInstanceOf[IntVector])
      assert(dataVector.asInstanceOf[StructVector].getChild("str").isInstanceOf[VarCharVector])

      prunedVector = columnarBatch.pruneFieldVectors(
        StructType(Seq(
          StructField("repeated",
            ArrayType(
              StructType(Seq(StructField("num", IntegerType, nullable = true)))
            )
          ))
        )
      )
      assertResult(1)(prunedVector.size())
      assert(prunedVector.get(0).isInstanceOf[ListVector])
      dataVector = prunedVector.get(0).asInstanceOf[ListVector].getDataVector
      assert(dataVector.isInstanceOf[StructVector])
      assertResult(1)(dataVector.asInstanceOf[StructVector].size())
      assert(dataVector.asInstanceOf[StructVector].getChild("num").isInstanceOf[IntVector])
    } finally {
      columnarBatch.close()
    }
  }

  test("Support repeated type") {
    val sqlText =
      """
        |  SELECT
        |    1000001 AS id,
        |    TIMESTAMP('2017-12-18 15:02:00') AS time,
        |    ARRAY(123, 4, 127, 9) AS bound
        |   UNION ALL
        |   SELECT
        |     1000003 AS id,
        |     TIMESTAMP('2017-12-17 12:12:00') AS time,
        |     ARRAY(128, 1, 130, 2) AS bound
        |""".stripMargin
    spark.sql(sqlText).write.saveAsTable("columnar.test")

    spark.sql(
      """
        |   SELECT
        |     1000002 AS id,
        |     TIMESTAMP('2017-12-16 11:34:00') AS time,
        |     null AS bound
        |""".stripMargin
    ).write.insertInto("columnar.test")

    val actual1 = spark.sql("SELECT id, bound FROM columnar.test ORDER BY id")
      .collect()
      .map(row => (row.getInt(0), if (row.get(1) == null) null else row.getSeq(1)))
    val expected1 = Array((1000001, Seq(123, 4, 127, 9)),
      (1000002, null),
      (1000003, Seq(128, 1, 130, 2)))
    assertResult(expected1)(actual1)

    val actual2 = spark.sql("SELECT id, time FROM columnar.test ORDER BY id")
      .collect()
      .map(row => (row.getInt(0), row.getTimestamp(1)))
    val expected2 = Array((1000001, java.sql.Timestamp.valueOf("2017-12-18 15:02:00")),
      (1000002, java.sql.Timestamp.valueOf("2017-12-16 11:34:00")),
      (1000003, java.sql.Timestamp.valueOf("2017-12-17 12:12:00")))
    assertResult(expected2)(actual2)
    dropTableAndCheckMemoryLeak("columnar", "test")
  }

  test("Support repeated nested type") {
    val sqlText =
      """
        |  SELECT
        |    1000001 AS id,
        |    STRUCT(TIMESTAMP('2017-12-18 15:02:00') AS start, TIMESTAMP('2017-12-18 15:42:00') AS end) AS time,
        |    ARRAY(
        |      STRUCT('ABC123456' AS sku, 'furniture' AS description, 3 AS quantity, 36.3f AS price),
        |      STRUCT('TBL535522' AS sku, 'table' AS description, 6 AS quantity, 878.4f AS price),
        |      STRUCT('CHR762222' AS sku, 'chair' AS description, 4 AS quantity, 435.6f AS price)
        |    ) AS product
        |   UNION ALL
        |   SELECT
        |     1000002 AS id,
        |     null AS time,
        |     ARRAY(
        |      STRUCT('GCH635354' AS sku, 'Chair' AS description, 4 AS quantity, 345.7f AS price),
        |      STRUCT('GRD828822' AS sku, 'Gardening'AS description, 2 AS quantity, 9.5f AS price)
        |     ) AS product
        |""".stripMargin
    spark.sql(sqlText).write.saveAsTable("columnar.test")

    spark.sql(
      """
        |   SELECT
        |     1000003 AS id,
        |     STRUCT(TIMESTAMP('2017-12-17 12:12:00') AS start, TIMESTAMP('2017-12-17 14:01:00') AS end) AS time,
        |     ARRAY() AS product
        |""".stripMargin
    ).write.insertInto("columnar.test")

    val actual = spark
      .sql("SELECT id, time.start, product.sku, product.quantity FROM columnar.test ORDER BY id")
      .collect()
      .map(row => (row.getInt(0), if (row.isNullAt(1)) null else row.getTimestamp(1), row.getSeq(2), row.getSeq(3)))
    val expected = Array(
      (1000001, java.sql.Timestamp.valueOf("2017-12-18 15:02:00"), Seq("ABC123456","TBL535522", "CHR762222"), Seq(3, 6, 4)),
      (1000002, null, Seq("GCH635354","GRD828822"), Seq(4, 2)),
      (1000003, java.sql.Timestamp.valueOf("2017-12-17 12:12:00"), Seq.empty[String], Seq.empty[Int]))
    assertResult(expected)(actual)
    dropTableAndCheckMemoryLeak("columnar", "test")
  }

  private def dropTableAndCheckMemoryLeak(catalogName: String, tableName: String): Unit = {
    catalog(catalogName).asInstanceOf[InMemoryTableCatalog].dropTable(
      Identifier.of(Array.empty[String], tableName))
    assert(OffHeapAllocator.bufferAllocator.getAllocatedMemory == 0)
  }
}
