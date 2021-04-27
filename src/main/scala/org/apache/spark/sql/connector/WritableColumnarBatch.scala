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

import org.apache.arrow.vector.{BigIntVector, BitVector, FieldVector, Float4Vector, Float8Vector, IntVector, SmallIntVector, TimeStampMicroTZVector, TinyIntVector, ValueVector, VarCharVector}
import org.apache.arrow.vector.complex.{ListVector, StructVector}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.SpecializedGetters
import org.apache.spark.sql.types.{ArrayType, StructType}

class WritableColumnarBatch(
    schema: StructType,
    val timeZone: String
) extends AutoCloseable {

  private var numRows: Int = 0

  private val writableColumnarVector: Array[FieldVector] =
    schema.fields.map {
      field =>
        ArrowAdapter.createFieldVector(field, OffHeapAllocator.bufferAllocator, timeZone)
    }

  override def close(): Unit = writableColumnarVector.foreach(vec => vec.close())

  def appendToValueVector(
      valueVector: ValueVector,
      getter: SpecializedGetters,
      startIndex: Int,
      numValues: Int
  ): Unit = {
    valueVector match {
      case vec: BitVector =>
        for (i <- 0 until numValues) {
          if (getter.isNullAt(i)) vec.setNull(startIndex + i)
          else vec.setSafe(startIndex + i, if (getter.getBoolean(i)) 1 else 0)
        }

      case vec: TinyIntVector =>
        for (i <- 0 until numValues) {
          if (getter.isNullAt(i)) vec.setNull(startIndex + i)
          else vec.setSafe(startIndex + i, getter.getByte(i))
        }

      case vec: SmallIntVector =>
        for (i <- 0 until numValues) {
          if (getter.isNullAt(i)) vec.setNull(startIndex + i)
          else vec.setSafe(startIndex + i, getter.getShort(i))
        }

      case vec: IntVector =>
        for (i <- 0 until numValues) {
          if (getter.isNullAt(i)) vec.setNull(startIndex + i)
          else vec.setSafe(startIndex + i, getter.getInt(i))
        }

      case vec: BigIntVector =>
        for (i <- 0 until numValues) {
          if (getter.isNullAt(i)) vec.setNull(startIndex + i)
          else vec.setSafe(startIndex + i, getter.getLong(i))
        }

      case vec: Float4Vector =>
        for (i <- 0 until numValues) {
          if (getter.isNullAt(i)) vec.setNull(startIndex + i)
          else vec.setSafe(startIndex + i, getter.getFloat(i))
        }

      case vec: Float8Vector =>
        for (i <- 0 until numValues) {
          if (getter.isNullAt(i)) vec.setNull(startIndex + i)
          else vec.setSafe(startIndex + i, getter.getDouble(i))
        }

      case vec: VarCharVector =>
        for (i <- 0 until numValues) {
          if (getter.isNullAt(i)) vec.setNull(startIndex + i)
          else vec.setSafe(startIndex + i, getter.getUTF8String(i).getBytes)
        }

      case vec: TimeStampMicroTZVector =>
        for (i <- 0 until numValues) {
          if (getter.isNullAt(i)) vec.setNull(startIndex + i)
          else vec.setSafe(startIndex + i, getter.getLong(i))
        }

      case vec: ListVector =>
        val childVec = vec.getChildrenFromFields.get(0)
        for (i <- 0 until numValues if !getter.isNullAt(i)) {
          val ret = vec.startNewValue(startIndex + i)
          appendToValueVector(childVec, getter.getArray(i), ret, getter.getArray(i).numElements())
          vec.endValue(startIndex + i, getter.getArray(i).numElements())
        }

      case vec: StructVector =>
        val childVec = vec.getChildrenFromFields
        for (i <- 0 until numValues if !getter.isNullAt(i)) {
          vec.setIndexDefined(startIndex + i)
          val columnarGetters = new ColumnarGetters(
            IndexedSeq(getter.getStruct(i, childVec.size())), childVec.size())
          for ((getter, j) <- columnarGetters.zipWithIndex) {
            appendToValueVector(childVec.get(j), getter, startIndex + i, 1)
          }
        }

      case vec =>
        throw new UnsupportedOperationException(s"Unsupported vector type: ${vec.getField}")
    }
    valueVector.setValueCount(startIndex + numValues)
  }

  def appendRows(internalRows: IndexedSeq[InternalRow]): Unit = {
    val columnarGetters = new ColumnarGetters(internalRows, schema.fields.length)
    for ((getter, i) <- columnarGetters.zipWithIndex) {
      appendToValueVector(writableColumnarVector(i), getter, numRows, internalRows.size)
    }
    numRows += internalRows.size
  }

  def pruneFieldVectors(requiredColumnsSchema: StructType): ju.List[FieldVector] = {
    val outputVectors = new ju.ArrayList[FieldVector]()
    requiredColumnsSchema.fields.foreach {
      sf =>
        val child = writableColumnarVector.find(vec => vec.getField.getName == sf.name).get
        val childVecToAdd = sf.dataType match {
          case st: StructType => new StructVectorView(child.asInstanceOf[StructVector], st)
          case at: ArrayType => new ListVectorView(child.asInstanceOf[ListVector], at)
          case _ => child
        }
        outputVectors.add(childVecToAdd)
    }
    outputVectors
  }

  def getRowCount(): Int = numRows
}
