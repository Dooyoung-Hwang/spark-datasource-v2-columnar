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

import java.{util => ju}
import org.apache.arrow.vector.{BigIntVector, BitVector, FieldVector, Float4Vector, Float8Vector, IntVector, SmallIntVector, TinyIntVector, VarCharVector, VectorSchemaRoot, VectorUnloader}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.ArrowUtils

import scala.collection.JavaConverters._

class WritableColumnarBatch(
    var numRows: Int,
    schema: StructType
) extends AutoCloseable {

  private val writableColumnarVector: Array[FieldVector] =
    schema.fields.map {
      field => val vec = ArrowAdapter.toArrowMinorType(field).getNewVector(
        ArrowUtils.toArrowField(field.name, field.dataType, field.nullable, null),
        OffHeapAllocator.bufferAllocator, null)
        vec
    }

  override def close(): Unit = writableColumnarVector.foreach(vec => vec.close())

  def appendRows(internalRows: Seq[InternalRow]): Unit = {
    for (ordinal <- schema.fields.indices) {
      writableColumnarVector(ordinal) match {
        case vec: BitVector =>
          internalRows.zipWithIndex.foreach {
            case (row, i) =>
              if (row.isNullAt(ordinal)) vec.setSafe(numRows + i, 0, 0)
              else vec.setSafe(numRows + i, if (row.getBoolean(ordinal)) 1 else 0)
          }
        case vec: TinyIntVector =>
          internalRows.zipWithIndex.foreach {
            case (row, i) =>
              if (row.isNullAt(ordinal)) vec.setSafe(numRows + i, 0, 0)
              else vec.setSafe(numRows + i, row.getByte(ordinal))
          }
        case vec: SmallIntVector =>
          internalRows.zipWithIndex.foreach {
            case (row, i) =>
              if (row.isNullAt(ordinal)) vec.setSafe(numRows + i, 0, 0)
              else vec.setSafe(numRows + i, row.getShort(ordinal))
          }
        case vec: IntVector =>
          internalRows.zipWithIndex.foreach {
            case (row, i) =>
              if (row.isNullAt(ordinal)) vec.setSafe(numRows + i, 0, 0)
              else vec.setSafe(numRows + i, row.getInt(ordinal))
          }
        case vec: BigIntVector =>
          internalRows.zipWithIndex.foreach {
            case (row, i) =>
              if (row.isNullAt(ordinal)) vec.setSafe(numRows + i, 0, 0)
              else vec.setSafe(numRows + i, row.getLong(ordinal))
          }
        case vec: Float4Vector =>
          internalRows.zipWithIndex.foreach {
            case (row, i) =>
              if (row.isNullAt(ordinal)) vec.setSafe(numRows + i, 0, 0)
              else vec.setSafe(numRows + i, row.getFloat(ordinal))
          }
        case vec: Float8Vector =>
          internalRows.zipWithIndex.foreach {
            case (row, i) =>
              if (row.isNullAt(ordinal)) vec.setSafe(numRows + i, 0, 0)
              else vec.setSafe(numRows + i, row.getDouble(ordinal))
          }
        case vec: VarCharVector =>
          internalRows.zipWithIndex.foreach {
            case (row, i) =>
              if (row.isNullAt(ordinal)) vec.setNull(numRows + i)
              else vec.setSafe(numRows + i, row.getUTF8String(ordinal).getBytes)
          }
        case vec =>
          throw new UnsupportedOperationException(s"Unsupported vector type: ${vec.getField}")
      }
    }
    numRows += internalRows.size
    writableColumnarVector.foreach(_.setValueCount(numRows))
  }

  def pruneFieldVectors(requiredFields: Array[String]): ju.List[FieldVector] = {
    requiredFields.toList
      .map(name => writableColumnarVector.find(vec => vec.getField.getName == name).get)
      .asJava
  }
}
