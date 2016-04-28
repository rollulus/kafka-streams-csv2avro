package com.eneco.energy.kafka.streams.csv

import org.apache.avro.Schema.Parser
import org.scalamock.scalatest.proxy.MockFactory
import org.scalatest.{FunSuite, Matchers}

class ColumnNameDrivenMapperUnitTest extends FunSuite with Matchers with MockFactory  {
  val schema = new Parser().parse(
    """
      |{
      |  "type": "record",
      |  "name": "TestPerson",
      |  "fields" : [
      |    {"name": "firstName", "type": "string"},
      |    {"name": "length", "type": "float"},
      |    {"name": "awesome", "type": "boolean"}
      |  ]
      |}
    """.stripMargin)

  val columns = "firstName,,,awesome,,length"

  val row = "rollulus,rouloul,male,true,1998,1.96"

  test("ColumnNameDrivenMapper.toGenericRecord should map appropriately") {
    val mapper = new ColumnNameDrivenMapper(schema, columns)
    val tgr = mapper.toGenericRecord(row.split(",").toSeq)
    tgr.isSuccess shouldBe true
    val r = tgr.get
    r.get("firstName") shouldEqual "rollulus"
    r.get("length") shouldBe Float.box(1.96f)
    r.get("awesome") shouldEqual true
  }
}