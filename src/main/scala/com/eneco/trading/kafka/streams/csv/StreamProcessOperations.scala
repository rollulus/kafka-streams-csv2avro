package com.eneco.energy.kafka.streams.csv

import org.apache.avro.Schema.Type
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.kafka.streams.kstream.KStream
import scala.collection.JavaConverters._
import scala.util.{Failure, Try}
import org.apache.avro.Schema

trait CsvToGenericRecordMapper {
  def toGenericRecord(csvTokens: Seq[String]): Try[GenericRecord]
}

trait CsvLineTokenizer {
  def tokenize(r: String): Seq[String]
}

class LameCommaCsvTokenizer extends CsvLineTokenizer {
  def tokenize(r: String): Seq[String] = r.split(",").map(_.trim).toSeq
}

class SchemaDrivenMapper(schema: Schema) extends CsvToGenericRecordMapper with Logging {
  require(schema.getType == Type.RECORD)

  val fields = schema.getFields.asScala.toSeq
  val fieldTypes = fields.map(field => if (field.schema.getType == Type.UNION)
        field.schema.getTypes.asScala.toSeq.map(_.getType) match {
        case Seq(Type.NULL, sect) => sect
        case _ => throw new Exception(s"Only unions of type [null, t] are supported, not `${field.schema}`")
      } else field.schema.getType)

  log.info("the mapping is: " + fields.zip(fieldTypes).map{case(f,t) => s"${f.name}:${t.getName}"}.mkString(", "))

  val fieldMappers = fields.zip(fieldTypes).map { case (field, fieldType) => fieldType match {
    case Type.STRING => (s: String) => (field.name, s)
    case Type.INT => (s: String) => (field.name, s.toInt)
    case Type.FLOAT => (s: String) => (field.name, s.toFloat)
    case Type.DOUBLE => (s: String) => (field.name, s.toDouble)
    case Type.BOOLEAN => (s: String) => (field.name, s.toBoolean)
    case _ => throw new Exception(s"cannot map `${field.name}` unsupported type `${fieldType}`")
  }}

  def toGenericRecord(csvTokens: Seq[String]): Try[GenericRecord] = {
    if (csvTokens.length != fieldMappers.length) {
      return Failure(new IllegalArgumentException(s"number of csv tokens ${csvTokens.length} does not match number of record fields ${fieldMappers.length}"))
    }
    Try {
      val genericRecord = new GenericData.Record(schema)
      csvTokens
        .zip(fieldMappers)
        .map { case (token, fieldMapperFn) => fieldMapperFn(token) }
        .foreach { case (fieldName, value) => genericRecord.put(fieldName, value) }
      genericRecord
    }
  }
}

class StreamingOperations(mapper: CsvToGenericRecordMapper, tokenizer: CsvLineTokenizer = new LameCommaCsvTokenizer) extends Logging {

  private def mapOrLogError(s: String) = {
    val v = mapper.toGenericRecord(tokenizer.tokenize(s))
    v match {
      case Failure(m) => log.warn(s"failed to parse `${s}`: ${m.toString}")
      case _ =>
    }
    v
  }

  def transform(csvRecords: KStream[String, String]) = csvRecords
      .mapValues[Try[GenericRecord]](mapOrLogError)
      .filter((k,v) => v.isSuccess)
      .mapValues(v=>v.get)
}
