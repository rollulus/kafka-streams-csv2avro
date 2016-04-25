package com.eneco.energy.kafka.streams.csv

import java.util
import io.confluent.kafka.serializers.KafkaAvroSerializer
import org.apache.kafka.common.Configurable
import org.apache.kafka.common.serialization.Serializer

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
