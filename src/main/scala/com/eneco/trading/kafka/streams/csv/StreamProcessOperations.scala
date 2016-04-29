package com.eneco.energy.kafka.streams.csv

import org.apache.avro.Schema.{Field, Type}
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.kafka.streams.kstream.KStream
import scala.collection.JavaConverters._
import scala.util.{Failure, Try}
import org.apache.avro.Schema

trait CsvToGenericRecordMapper {
  def toGenericRecord(csvRecord: String): Try[GenericRecord]
}

trait CsvLineTokenizer {
  def tokenize(r: String): Seq[String]
}

trait StringParsers {
  def get(t: Type): (String) => Option[Any]
}

class RegexCsvTokenizer(regex:String = ",") extends CsvLineTokenizer {
  def tokenize(r: String): Seq[String] = r.split(regex).map(_.trim).toSeq
}


object StringParsers {
  val defaults = Map[Type, (String) => Option[Any]](
    (Type.STRING, (s: String) => Some(s)),
    (Type.FLOAT, (s: String) => Some(s.toFloat)),
    (Type.BOOLEAN, (s: String) => Some(s.toBoolean)),
    (Type.DOUBLE, (s: String) => Some(s.toDouble)),
    (Type.INT, (s: String) => Some(s.toInt))
  )
}

class ColumnNameDrivenMapper(schema: Schema,
                             columnNamePattern: String,
                             tokenizer: CsvLineTokenizer = new RegexCsvTokenizer,
                             stringParsers: Map[Type, (String) => Option[Any]] = StringParsers.defaults) extends CsvToGenericRecordMapper with Logging {
  require(schema.getType == Type.RECORD)

  // for record: maps field name -> Field
  val recordFields: Map[String, Field] = schema.getFields.asScala.toSeq.map(f => (f.name, f)).toMap

  // for csv: Seq[(name, #column)]
  val columnNames: Seq[(String, Int)] = interpretColumnNamePattern(columnNamePattern)

  // sanity check: all columns should be fields of record
  columnNames.foreach {
    case (columnName, _) => require(recordFields.contains(columnName), s"column `${columnName}` must be a field of record `${schema.getName}`")
  }

  // for csv: Seq[(name, #column, Type)]
  val columns: Seq[(String, Int, Type)] = columnNames.map { case (columnName, i) => (columnName, i, inferFieldType(recordFields(columnName))) }

  // give debug info: what maps onto what
  columns
    .map { case (columnName, i, fieldType) => log.info(s"csv column ${i} will be mapped to ${schema.getName}.${columnName}:${fieldType.getName}") }

  // a map of columnName -> stringParser functions
  val mapperFunctions: Map[String, (Seq[String]) => Option[Any]] = columns
    .map { case (columnName, i, fieldType) =>
      stringParsers.get(fieldType) match {
        case Some(parser) => (columnName, (s: Seq[String]) => parser(s(i)))
        case _ => throw new Exception("TODO") //TODO
      }
    }.toMap

  def interpretColumnNamePattern(pattern: String): Seq[(String, Int)] = tokenizer.tokenize(pattern)
    .map(_.trim)
    .zipWithIndex
    .filterNot { case (columnName, i) => columnName.isEmpty }

  def inferFieldType(field: Schema.Field): Type = if (field.schema.getType == Type.UNION)
    field.schema.getTypes.asScala.toSeq.map(_.getType) match {
      case Seq(Type.NULL, sect) => sect
      case _ => throw new Exception(s"Only unions of type [null, t] are supported, not `${field.schema}`")
    } else field.schema.getType

  def toGenericRecord(csvRecord: String): Try[GenericRecord] = {
    Try {
      val genericRecord = new GenericData.Record(schema)
      val csvTokens = tokenizer.tokenize(csvRecord)
      mapperFunctions
        .map { case (name, function) => (name, function(csvTokens)) }
        .foreach { case (name, Some(value)) => genericRecord.put(name, value) }
      genericRecord
    }
  }
}

class StreamingOperations(mapper: CsvToGenericRecordMapper) extends Logging {
  private def mapOrLogError(s: String) = {
    val v = mapper.toGenericRecord(s)
    v match {
      case Failure(m) => log.warn(s"failed to parse `${s}`: ${m.toString}")
      case _ =>
    }
    v
  }

  def transform(csvRecords: KStream[String, String]) = csvRecords
    .mapValues[Try[GenericRecord]](mapOrLogError)
    .filter((k, v) => v.isSuccess)
    .mapValues(v => v.get)
}
