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

import org.apache.arrow.vector.complex.{ListVector, StructVector}

import org.apache.spark.sql.types.{ArrayType, StructType}

class ListVectorView(source: ListVector, listToView: ArrayType)
  extends ListVector(source.getName, source.getAllocator, source.getField.getFieldType, null) {

  this.validityBuffer = source.getValidityBuffer
  this.offsetBuffer = source.getOffsetBuffer
  this.valueCount = source.getValueCount

  listToView.elementType match {
    case at: ArrayType =>
      this.vector = new ListVectorView(source.getDataVector.asInstanceOf[ListVector], at)
    case st: StructType =>
      this.vector = new StructVectorView(source.getDataVector.asInstanceOf[StructVector], st)
    case _ =>
      this.vector = source.getDataVector
  }

  override def close(): Unit = {
    throw new UnsupportedOperationException("ListVectorView does not support closing")
  }
}
