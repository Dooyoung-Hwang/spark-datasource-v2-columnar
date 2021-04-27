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

import java.time.temporal.ChronoUnit
import java.time.{Instant, ZoneId}
import java.{util => ju}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.connector.catalog.{SupportsRead, SupportsWrite, Table, TableCapability}
import org.apache.spark.sql.connector.expressions.{BucketTransform, DaysTransform, HoursTransform, IdentityTransform, MonthsTransform, Transform, YearsTransform}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write._
import org.apache.spark.sql.sources.{And, EqualTo, Filter, IsNotNull}
import org.apache.spark.sql.types.{DataType, DateType, StructType, TimestampType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.collection.JavaConverters._
import scala.collection.mutable

class InMemoryColumnarTable(
    val name: String,
    val schema: StructType,
    override val partitioning: Array[Transform],
    override val properties: ju.Map[String, String])
  extends Table with SupportsWrite with SupportsRead with AutoCloseable {

  partitioning.foreach {
    case _: IdentityTransform =>
    case _: YearsTransform =>
    case _: MonthsTransform =>
    case _: DaysTransform =>
    case _: HoursTransform =>
    case _: BucketTransform =>
    case t  =>
      throw new IllegalArgumentException(s"Transform $t is not a supported transform")
  }

  // The key `Seq[Any]` is the partition values.
  val dataMap: mutable.Map[Seq[Any], WritableColumnarBatch] = mutable.Map.empty

  private val partCols: Array[Array[String]] = partitioning.flatMap(_.references).map { ref =>
    schema.findNestedField(ref.fieldNames(), includeCollections = false) match {
      case Some(_) => ref.fieldNames()
      case None => throw new IllegalArgumentException(s"${ref.describe()} does not exist.")
    }
  }

  private val UTC = ZoneId.of("UTC")
  private val EPOCH_LOCAL_DATE = Instant.EPOCH.atZone(UTC).toLocalDate

  private def getKey(row: InternalRow): Seq[Any] = {
    def extractor(
        fieldNames: Array[String],
        schema: StructType,
        row: InternalRow): (Any, DataType) = {
      val index = schema.fieldIndex(fieldNames(0))
      val value = row.toSeq(schema).apply(index)
      if (fieldNames.length > 1) {
        (value, schema(index).dataType) match {
          case (row: InternalRow, nestedSchema: StructType) =>
            extractor(fieldNames.drop(1), nestedSchema, row)
          case (_, dataType) =>
            throw new IllegalArgumentException(s"Unsupported type, ${dataType.simpleString}")
        }
      } else {
        (value, schema(index).dataType)
      }
    }

    partitioning.map {
      case IdentityTransform(ref) =>
        extractor(ref.fieldNames, schema, row)._1
      case YearsTransform(ref) =>
        extractor(ref.fieldNames, schema, row) match {
          case (days: Int, DateType) =>
            ChronoUnit.YEARS.between(EPOCH_LOCAL_DATE, DateTimeUtils.daysToLocalDate(days))
          case (micros: Long, TimestampType) =>
            val localDate = DateTimeUtils.microsToInstant(micros).atZone(UTC).toLocalDate
            ChronoUnit.YEARS.between(EPOCH_LOCAL_DATE, localDate)
        }
      case MonthsTransform(ref) =>
        extractor(ref.fieldNames, schema, row) match {
          case (days: Int, DateType) =>
            ChronoUnit.MONTHS.between(EPOCH_LOCAL_DATE, DateTimeUtils.daysToLocalDate(days))
          case (micros: Long, TimestampType) =>
            val localDate = DateTimeUtils.microsToInstant(micros).atZone(UTC).toLocalDate
            ChronoUnit.MONTHS.between(EPOCH_LOCAL_DATE, localDate)
        }
      case DaysTransform(ref) =>
        extractor(ref.fieldNames, schema, row) match {
          case (days, DateType) =>
            days
          case (micros: Long, TimestampType) =>
            ChronoUnit.DAYS.between(Instant.EPOCH, DateTimeUtils.microsToInstant(micros))
        }
      case HoursTransform(ref) =>
        extractor(ref.fieldNames, schema, row) match {
          case (micros: Long, TimestampType) =>
            ChronoUnit.HOURS.between(Instant.EPOCH, DateTimeUtils.microsToInstant(micros))
        }
      case BucketTransform(numBuckets, ref) =>
        (extractor(ref.fieldNames, schema, row).hashCode() & Integer.MAX_VALUE) % numBuckets
    }
  }

  override def close(): Unit = {
    dataMap.values.foreach(vec => vec.close())
  }

  def withData(data: Array[BufferedRows]): InMemoryColumnarTable = dataMap.synchronized {
    val rowsGroupByPartition: mutable.Map[Seq[Any], BufferedRows] =
      new mutable.HashMap[Seq[Any], BufferedRows]()
    data.foreach {
      _.rows.foreach { row =>
        val key = getKey(row)
        rowsGroupByPartition +=
          ((key, rowsGroupByPartition.get(key)
            .map(_.withRow(row))
            .getOrElse(new BufferedRows().withRow(row))))
      }
    }
    rowsGroupByPartition.foreach {
      case (key, rows) =>
        val rb = dataMap.getOrElseUpdate(key, new WritableColumnarBatch(schema, UTC.getId))
        rb.appendRows(rows.rows)
    }
    this
  }

  def withData(data: Map[Seq[Any], WritableColumnarBatch]): InMemoryColumnarTable =
    dataMap.synchronized {
      for ((key, vectors) <- data) {
        dataMap += ((key, vectors))
      }
      this
    }

  override def capabilities(): ju.Set[TableCapability] = Set(
    TableCapability.BATCH_READ,
    TableCapability.BATCH_WRITE,
    TableCapability.OVERWRITE_BY_FILTER,
    TableCapability.OVERWRITE_DYNAMIC,
    TableCapability.TRUNCATE).asJava

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    new InMemoryTableScanBuilder {
      override def buildScanPartitions(
          requiredColumnsSchema: StructType
      ): Array[InMemoryTableScanPartition] = dataMap.synchronized {
        dataMap.values.map {
          columnarBatch: WritableColumnarBatch =>
            if (requiredColumnsSchema.fields.isEmpty) {
              InMemoryTableScanPartition(
                ArrowAdapter.emptyArrowSchemaBinary(),
                ArrowAdapter.emptyFieldVectorsBinary(columnarBatch.getRowCount()))
            } else {
              val fieldVectors = columnarBatch.pruneFieldVectors(requiredColumnsSchema)
              InMemoryTableScanPartition(requiredColumnsSchema, fieldVectors,
                columnarBatch.timeZone)
            }
        }.toArray
      }
    }
  }

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    new InMemoryTableWriteBuilder {
      override def truncateAndAppend(rows: Array[BufferedRows]): Unit = dataMap.synchronized {
        dataMap.values.foreach(_.close())
        dataMap.clear
        withData(rows)
      }

      override def append(rows: Array[BufferedRows]): Unit = dataMap.synchronized {
        withData(rows)
      }

      override def deleteExistingPartitionsAndAppend(rows: Array[BufferedRows]): Unit = dataMap.synchronized {
        val deleteKeys = rows.flatMap(_.rows.map(getKey))
        deleteKeys.foreach(key => dataMap.remove(key).map(_.close()))
        withData(rows)
      }

      override def deleteMatchingPartitionsWithFilterAndAppend(
          rows: Array[BufferedRows],
          filters: Array[Filter]
      ): Unit = dataMap.synchronized {
        import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.MultipartIdentifierHelper
        val deleteKeys = InMemoryColumnarTable.filtersToKeys(
          dataMap.keys, partCols.map(_.toSeq.quoted), filters)
        deleteKeys.foreach(key => dataMap.remove(key).map(_.close()))
        withData(rows)
      }
    }
  }
}

object InMemoryColumnarTable {

  def filtersToKeys(
      keys: Iterable[Seq[Any]],
      partitionNames: Seq[String],
      filters: Array[Filter]): Iterable[Seq[Any]] = {
    keys.filter { partValues =>
      filters.flatMap(splitAnd).forall {
        case EqualTo(attr, value) =>
          value == extractValue(attr, partitionNames, partValues)
        case IsNotNull(attr) =>
          null != extractValue(attr, partitionNames, partValues)
        case f =>
          throw new IllegalArgumentException(s"Unsupported filter type: $f")
      }
    }
  }

  private def extractValue(
      attr: String,
      partFieldNames: Seq[String],
      partValues: Seq[Any]): Any = {
    partFieldNames.zipWithIndex.find(_._1 == attr) match {
      case Some((_, partIndex)) =>
        partValues(partIndex)
      case _ =>
        throw new IllegalArgumentException(s"Unknown filter attribute: $attr")
    }
  }

  private def splitAnd(filter: Filter): Seq[Filter] = {
    filter match {
      case And(left, right) => splitAnd(left) ++ splitAnd(right)
      case _ => filter :: Nil
    }
  }
}
