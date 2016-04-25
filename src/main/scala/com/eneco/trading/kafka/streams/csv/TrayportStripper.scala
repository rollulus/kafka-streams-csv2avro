package com.eneco.energy.kafka.streams.csv

import java.io.File

import io.confluent.kafka.serializers.{KafkaAvroSerializer, KafkaAvroDeserializerConfig}
import org.apache.avro.Schema
import org.apache.avro.Schema.Parser
import org.apache.avro.generic.GenericData.StringType
import org.apache.avro.generic.{GenericRecord, GenericData, GenericDatumWriter, GenericRecordBuilder}
import org.apache.kafka.common.serialization._
import org.apache.kafka.streams.kstream.KStreamBuilder
import org.apache.kafka.streams._
import Properties._
import io.confluent.kafka.serializers.{KafkaAvroSerializer, KafkaAvroDeserializer}
import org.apache.kafka.common.Configurable
import org.apache.kafka.common.serialization.{Serializer, Deserializer}
import java.util
import scala.collection.JavaConverters._


// NOTE: Must have a public no-argument constructor (org.apache.kafka.common.utils.Utils.newInstance)
class GenericAvroSerializer[T]() extends Serializer[T] with Configurable {
  private val inner: KafkaAvroSerializer = new KafkaAvroSerializer()

  def this(map: util.Map[String, _]) {
    this()
    configure(map)
  }

  def configure(map: util.Map[String, _], b: Boolean): Unit = {
    inner.configure(map, b)
  }

  def serialize(t: String, v: T): Array[Byte] = {
    inner.serialize(t, v)
  }

  def close(): Unit = inner.close

  def configure(map: util.Map[String, _]): Unit = {
    inner.configure(map, false)
  }
}

object StreamProcessor {
  lazy val SOURCE_TOPIC_CONFIG = "source.topic"
  lazy val SINK_TOPIC_CONFIG = "sink.topic"
  lazy val SCHEMA_FILE_CONFIG = "schema.file"

  def main(args: Array[String]): Unit = {
    // configure
    require(args.length > 0, "at least one .properties file should be given as program argument")
    val builder = new KStreamBuilder
    val cfg = propertiesFromFiles(args) | fixedProperties
    val sourceTopic = cfg.getProperty(SOURCE_TOPIC_CONFIG)
    val sinkTopic = cfg.getProperty(SINK_TOPIC_CONFIG)
    val destSchema = new Parser().parse(new File(cfg.getProperty(SCHEMA_FILE_CONFIG)))
    val mapper = new SchemaDrivenMapper(destSchema)

    // source: GV8APIDATAs
    val in = builder.stream[String, String](sourceTopic)

    // transformations
    val out = new StreamingOperations(mapper).transform(in)

    val kas = new KafkaAvroSerializer
    kas.configure(cfg.toHashMap, false)

    // sinks
    val cfgMap = cfg.toHashMap
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

