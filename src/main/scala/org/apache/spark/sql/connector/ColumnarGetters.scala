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

import org.apache.spark.sql.catalyst.expressions.SpecializedGetters
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
import org.apache.spark.sql.types.{DataType, Decimal}
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}

class ColumnarGetters(internalRows: IndexedSeq[InternalRow], numColumns: Int)
  extends Iterator[SpecializedGetters] {
  private var columnIndex = 0

  override def hasNext: Boolean = columnIndex < numColumns

  override def next(): SpecializedGetters = {
    val getter: SpecializedGetters = new SpecializedGetters {
      private val index = columnIndex

      override def isNullAt(ordinal: Int): Boolean = internalRows(ordinal).isNullAt(index)

      override def getBoolean(ordinal: Int): Boolean = internalRows(ordinal).getBoolean(index)

      override def getByte(ordinal: Int): Byte = internalRows(ordinal).getByte(index)

      override def getShort(ordinal: Int): Short = internalRows(ordinal).getShort(index)

      override def getInt(ordinal: Int): Int = internalRows(ordinal).getInt(index)

      override def getLong(ordinal: Int): Long = internalRows(ordinal).getLong(index)

      override def getFloat(ordinal: Int): Float = internalRows(ordinal).getFloat(index)

      override def getDouble(ordinal: Int): Double = internalRows(ordinal).getDouble(index)

      override def getDecimal(ordinal: Int, precision: Int, scale: Int): Decimal = {
        internalRows(ordinal).getDecimal(index, precision, scale)
      }

      override def getUTF8String(ordinal: Int): UTF8String = {
        internalRows(ordinal).getUTF8String(index)
      }

      override def getBinary(ordinal: Int): Array[Byte] = {
        internalRows(ordinal).getBinary(index)
      }

      override def getInterval(ordinal: Int): CalendarInterval = {
        internalRows(ordinal).getInterval(index)
      }

      override def getStruct(ordinal: Int, numFields: Int): InternalRow = {
        internalRows(ordinal).getStruct(index, numFields)
      }

      override def getArray(ordinal: Int): ArrayData = {
        internalRows(ordinal).getArray(index)
      }

      override def getMap(ordinal: Int): MapData = {
        internalRows(ordinal).getMap(index)
      }

      override def get(ordinal: Int, dataType: DataType): AnyRef = {
        internalRows(ordinal).get(index, dataType)
      }
    }
    columnIndex += 1
    getter
  }
}
