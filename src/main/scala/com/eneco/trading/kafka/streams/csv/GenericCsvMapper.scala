package com.eneco.energy.kafka.streams.csv

import java.io.File
import org.apache.avro.Schema.Parser
import org.apache.avro.generic.{GenericRecord}
import org.apache.kafka.common.serialization._
import org.apache.kafka.streams.kstream.KStreamBuilder
import org.apache.kafka.streams._
import Properties._
import io.confluent.kafka.serializers.{KafkaAvroSerializer}
import org.apache.kafka.common.Configurable
import org.apache.kafka.common.serialization.{Serializer}
import java.util

object StreamProcessor {
  lazy val SOURCE_TOPIC_CONFIG = "source.topic"
  lazy val SINK_TOPIC_CONFIG = "sink.topic"
  lazy val SCHEMA_FILE_CONFIG = "schema.file"
  lazy val CSV_COLUMNS_CONFIG = "csv.columns"
  lazy val CSV_SEPARATOR_REGEX_CONFIG = "csv.separator"
  lazy val CSV_SEPARATOR_REGEX_DEFAULT = ","

  def main(args: Array[String]): Unit = {
    // configure
    require(args.length > 0, "at least one .properties file should be given as program argument")
    val builder = new KStreamBuilder
    val cfg = propertiesFromFiles(args) | fixedProperties
    val sourceTopic = cfg.getProperty(SOURCE_TOPIC_CONFIG)
    val sinkTopic = cfg.getProperty(SINK_TOPIC_CONFIG)
    val destSchema = new Parser().parse(new File(cfg.getProperty(SCHEMA_FILE_CONFIG)))
    val csvTokenizer: RegexCsvTokenizer = new RegexCsvTokenizer(cfg.getProperty(CSV_SEPARATOR_REGEX_CONFIG, CSV_SEPARATOR_REGEX_DEFAULT))
    val mapper: ColumnNameDrivenMapper = new ColumnNameDrivenMapper(destSchema, cfg.getProperty(CSV_COLUMNS_CONFIG), csvTokenizer)

    // source
    val in = builder.stream[String, String](sourceTopic)

    // transformations
    val out = new StreamingOperations(mapper).transform(in)

    // sinks
    out.to(sinkTopic)

    // run
    new KafkaStreams(builder, cfg).start()
  }

  def propertiesFromFiles(files: Array[String]) = files.map(Properties.fromFile).foldLeft(new java.util.Properties)(_ | _)

  def fixedProperties() = Properties.create(Map(
    StreamsConfig.KEY_SERIALIZER_CLASS_CONFIG -> classOf[StringSerializer],
    StreamsConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
    StreamsConfig.VALUE_SERIALIZER_CLASS_CONFIG -> classOf[GenericAvroSerializer[GenericRecord]],
    StreamsConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer]
  ))

}

