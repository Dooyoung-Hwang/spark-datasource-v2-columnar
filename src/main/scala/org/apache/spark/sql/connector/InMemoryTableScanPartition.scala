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

import org.apache.arrow.vector.FieldVector
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.types.StructType

case class InMemoryTableScanPartition(arrowSchema: Array[Byte], recordBatch: Array[Byte])
  extends InputPartition

object InMemoryTableScanPartition {
  def apply(
      schema: StructType,
      fieldVectors: ju.List[FieldVector],
      timeZone: String
  ): InMemoryTableScanPartition = {
    val schemaBinary: Array[Byte] = ArrowAdapter.serializeStructTypeToArrowSchemaBinary(
      schema, timeZone)
    val recordBatchBinary = ArrowAdapter.serializeFieldVectors(fieldVectors)
    InMemoryTableScanPartition(schemaBinary, recordBatchBinary)
  }
}
