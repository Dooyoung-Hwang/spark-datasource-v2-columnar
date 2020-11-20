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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.{BatchWrite, DataWriter, DataWriterFactory, PhysicalWriteInfo, SupportsDynamicOverwrite, SupportsOverwrite, SupportsTruncate, WriteBuilder, WriterCommitMessage}
import org.apache.spark.sql.sources.Filter

import scala.collection.mutable

trait InMemoryTableWriteBuilder extends WriteBuilder
  with SupportsTruncate with SupportsOverwrite with SupportsDynamicOverwrite {
  private var writer: BatchWrite = Append

  override def truncate(): WriteBuilder = {
    assert(writer == Append)
    writer = TruncateAndAppend
    this
  }

  override def overwrite(filters: Array[Filter]): WriteBuilder = {
    assert(writer == Append)
    writer = new Overwrite(filters)
    this
  }

  override def overwriteDynamicPartitions(): WriteBuilder = {
    assert(writer == Append)
    writer = DynamicOverwrite
    this
  }

  override def buildForBatch(): BatchWrite = writer

  private abstract class InMemoryTableBatchWrite extends BatchWrite {
    override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory = {
      BufferedRowsWriterFactory
    }

    override def abort(messages: Array[WriterCommitMessage]): Unit = {}
  }

  private object Append extends InMemoryTableBatchWrite {
    override def commit(messages: Array[WriterCommitMessage]): Unit =
      append(messages.map(_.asInstanceOf[BufferedRows]))
  }

  private object DynamicOverwrite extends InMemoryTableBatchWrite {
    override def commit(messages: Array[WriterCommitMessage]): Unit =
      deleteExistingPartitionsAndAppend(messages.map(_.asInstanceOf[BufferedRows]))
  }

  private class Overwrite(filters: Array[Filter]) extends InMemoryTableBatchWrite {
    override def commit(messages: Array[WriterCommitMessage]): Unit =
      deleteMatchingPartitionsWithFilterAndAppend(
        messages.map(_.asInstanceOf[BufferedRows]), filters)
  }

  private object TruncateAndAppend extends InMemoryTableBatchWrite {
    override def commit(messages: Array[WriterCommitMessage]): Unit =
      truncateAndAppend(messages.map(_.asInstanceOf[BufferedRows]))
  }

  def truncateAndAppend(rows: Array[BufferedRows]): Unit

  def append(rows: Array[BufferedRows]): Unit

  def deleteExistingPartitionsAndAppend(rows: Array[BufferedRows]): Unit

  def deleteMatchingPartitionsWithFilterAndAppend(
      rows: Array[BufferedRows], filters: Array[Filter]): Unit
}


class BufferedRows extends WriterCommitMessage with Serializable {
  val rows = new mutable.ArrayBuffer[InternalRow]()

  def withRow(row: InternalRow): BufferedRows = {
    rows.append(row)
    this
  }
}

private object BufferedRowsWriterFactory extends DataWriterFactory {
  override def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow] = {
    new BufferWriter
  }
}

private class BufferWriter extends DataWriter[InternalRow] {
  private val buffer = new BufferedRows

  override def write(row: InternalRow): Unit = buffer.rows.append(row.copy())

  override def commit(): WriterCommitMessage = buffer

  override def abort(): Unit = {}

  override def close(): Unit = {}
}
