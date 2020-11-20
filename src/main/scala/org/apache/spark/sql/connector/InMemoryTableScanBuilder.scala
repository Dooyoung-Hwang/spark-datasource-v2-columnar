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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReader, PartitionReaderFactory, Scan, ScanBuilder, SupportsPushDownRequiredColumns}
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.vectorized.ColumnarBatch

import scala.collection.JavaConverters._

trait InMemoryTableScanBuilder extends ScanBuilder with SupportsPushDownRequiredColumns  {
  private var requiredColumnsSchema: StructType = StructType(Array.empty[StructField])

  override def pruneColumns(requiredSchema: StructType): Unit = {
    requiredColumnsSchema = requiredSchema
  }

  def buildScanPartitions(requiredColumnsSchema: StructType): Array[InMemoryTableScanPartition]

  override def build(): Scan = new InMemoryBatchScan(
    buildScanPartitions(requiredColumnsSchema).asInstanceOf[Array[InputPartition]],
    requiredColumnsSchema
  )
}

class InMemoryBatchScan(
    partitions: Array[InputPartition],
    schemaOfPrunedColumns: StructType
) extends Scan with Batch {
  override def readSchema(): StructType = schemaOfPrunedColumns

  override def toBatch: Batch = this

  override def planInputPartitions(): Array[InputPartition] = partitions

  override def createReaderFactory(): PartitionReaderFactory = ArrowColumnarBatchReaderFactory
}

object ArrowColumnarBatchReaderFactory extends PartitionReaderFactory {
  private def buildArrowColumnarBatch(partition: InMemoryTableScanPartition): ColumnarBatch = {
    ArrowAdapter.deserializeRecordBatchInputStream(
      ArrowAdapter.deserializeArrowSchemaBinary(partition.arrowSchema),
      new ByteArrayInputStream(partition.recordBatch),
      OffHeapAllocator.bufferAllocator
    )
  }

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] =
    new PartitionReader[InternalRow] {
      private val columnarBatch: ColumnarBatch = buildArrowColumnarBatch(
        partition.asInstanceOf[InMemoryTableScanPartition])

      private val sourceIterator: Iterator[InternalRow] = columnarBatch.rowIterator().asScala

      override def next(): Boolean = sourceIterator.hasNext

      override def get(): InternalRow = sourceIterator.next()

      override def close(): Unit = columnarBatch.close()
    }

  override def createColumnarReader(partition: InputPartition): PartitionReader[ColumnarBatch] =
    new PartitionReader[ColumnarBatch] {
      private val columnarBatch: ColumnarBatch = buildArrowColumnarBatch(
        partition.asInstanceOf[InMemoryTableScanPartition])

      private val sourceIterator: Iterator[ColumnarBatch] = Iterator.single(columnarBatch)

      override def next(): Boolean = sourceIterator.hasNext

      override def get(): ColumnarBatch = sourceIterator.next()

      override def close(): Unit = columnarBatch.close()
    }

  override def supportColumnarReads(partition: InputPartition): Boolean = true
}

