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

import java.io.{ByteArrayOutputStream, InputStream}
import java.{util => ju}
import java.nio.ByteBuffer
import java.nio.channels.Channels

import io.netty.buffer.ArrowBuf
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.util.AutoCloseables
import org.apache.arrow.vector.{FieldVector, VectorLoader, VectorSchemaRoot, VectorUnloader}
import org.apache.arrow.vector.ipc.{ReadChannel, WriteChannel}
import org.apache.arrow.vector.ipc.message.{ArrowFieldNode, ArrowRecordBatch, MessageSerializer}
import org.apache.arrow.vector.types.Types.MinorType
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.spark.sql.types.{BinaryType, BooleanType, ByteType, DateType, DecimalType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnarBatch}

import scala.collection.JavaConverters._
import scala.collection.mutable

object ArrowAdapter {

  def toArrowMinorType(field: StructField): MinorType = field.dataType match {
    case BooleanType => MinorType.BIT
    case ByteType => MinorType.TINYINT
    case ShortType => MinorType.SMALLINT
    case IntegerType => MinorType.INT
    case LongType => MinorType.BIGINT
    case FloatType => MinorType.FLOAT4
    case DoubleType => MinorType.FLOAT8
    case StringType => MinorType.VARCHAR
    case BinaryType => MinorType.VARBINARY
    case _: DecimalType => MinorType.DECIMAL
    case DateType => MinorType.DATEDAY
    case TimestampType => MinorType.TIMESTAMPMICRO
    case _ =>
      MinorType.STRUCT
      throw new UnsupportedOperationException(s"Unsupported data type: ${field.dataType}")
  }

  def serializeStructTypeToArrowSchemaBinary(
      schema: StructType,
      timeZoneId: String
  ): Array[Byte] = {
    ArrowUtils.toArrowSchema(schema, timeZoneId).toByteArray
  }

  def deserializeArrowSchemaBinary(serializedSchema: Array[Byte]): Schema = {
    Schema.deserialize(ByteBuffer.wrap(serializedSchema))
  }

  def deserializeArrowSchemaBinaryToStructType(
      serializedSchema: Array[Byte]
  ): StructType = {
    ArrowUtils.fromArrowSchema(Schema.deserialize(ByteBuffer.wrap(serializedSchema)))
  }

  def serializeFieldVectors(fieldVectors: ju.List[FieldVector]): Array[Byte] = {
    val closeables = new mutable.ListBuffer[AutoCloseable]()
    try {
      val binaryOutputStream = new ByteArrayOutputStream
      val unloader = new VectorUnloader(new VectorSchemaRoot(fieldVectors))
      val recordBatch = unloader.getRecordBatch
      closeables += recordBatch
      MessageSerializer.serialize(
        new WriteChannel(Channels.newChannel(binaryOutputStream)), recordBatch)
      binaryOutputStream.toByteArray
    } finally {
      AutoCloseables.close(closeables.asJava)
    }
  }

  def deserializeRecordBatchInputStream(
      arrowSchema: Schema,
      recordBatchInputStream: InputStream,
      allocator: BufferAllocator
  ): ColumnarBatch = {
    val closeables = new mutable.ListBuffer[AutoCloseable]()
    try {
      val deserializedRecordBatch: ArrowRecordBatch = MessageSerializer.deserializeRecordBatch(
        new ReadChannel(Channels.newChannel(recordBatchInputStream)),
        allocator)
      closeables += deserializedRecordBatch
      val schemaRoot = VectorSchemaRoot.create(arrowSchema, allocator)
      val vectorLoader = new VectorLoader(schemaRoot)
      vectorLoader.load(deserializedRecordBatch)
      new ColumnarBatch(schemaRoot.getFieldVectors.asScala
        .map(vec => new ArrowColumnVector(vec))
        .toArray,
        schemaRoot.getRowCount
      )
    } finally {
      AutoCloseables.close(closeables.asJava)
    }
  }

  def emptyArrowSchemaBinary(): Array[Byte] = {
    ArrowUtils.toArrowSchema(StructType(Array.empty[StructField]), null).toByteArray
  }

  def emptyFieldVectorsBinary(numRows: Int): Array[Byte] = {
    val closeables = new mutable.ListBuffer[AutoCloseable]()
    try {
      val binaryOutputStream = new ByteArrayOutputStream
      val recordBatch = new ArrowRecordBatch(numRows, List.empty[ArrowFieldNode].asJava,
        List.empty[ArrowBuf].asJava)
      closeables += recordBatch
      MessageSerializer.serialize(
        new WriteChannel(Channels.newChannel(binaryOutputStream)), recordBatch)
      binaryOutputStream.toByteArray
    } finally {
      AutoCloseables.close(closeables.asJava)
    }
  }
}
