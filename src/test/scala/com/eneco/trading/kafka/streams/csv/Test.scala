package com.eneco.energy.kafka.streams.csv

import org.apache.avro.Schema
import org.apache.avro.Schema.Parser
import org.scalamock.scalatest.proxy.MockFactory
import org.scalatest.{FunSuite, Matchers}

class ColumnNameDrivenMapperSmokeTest extends FunSuite with Matchers with MockFactory  {
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

  def verify(props:String, schema: Schema, rows: Seq[String], expectedFields:Seq[Map[String, Any]]) = {
    val mapper = StreamProcessor.mapperFromProperties(Properties.fromString(props),Some(schema))
    val records = rows.map(mapper.toGenericRecord).map(_.get)
      .zip(expectedFields).foreach {
      case (rec, exp) => exp.foreach { case (k, v) => rec.get(k) shouldEqual v }
    }

  }
  val expectedFields =
    Seq(Map("firstName" -> "rollulus", "length" -> Float.box(1.96f), "awesome" -> true),
      Map("firstName" -> "felix", "length" -> Float.box(0.23f), "awesome" -> false))

  test("ColumnNameDrivenMapper.toGenericRecord should map appropriately using defaults") {
    val rows =
      """rollulus,rouloul,male,true ,1998,1.96
        |felix, domesticus,female,false,1888,.23
        |""".stripMargin.split("\n").toSeq

    verify(
      """csv.columns=firstName,,,awesome,,length
        |""".stripMargin, schema, rows, expectedFields)
  }

  test("ColumnNameDrivenMapper.toGenericRecord should adhere `string.regex`") {
    val rows =
      """"rollulus",rouloul,male, true ,1998,1.96
        |felix ,domesticus,female,false,1888,.23
        |""".stripMargin.split("\n").toSeq

    verify(
      """csv.columns=firstName,,,awesome,,length
        |string.regex="?([^"]*)"?
        |""".stripMargin, schema, rows, expectedFields)
  }

  test("ColumnNameDrivenMapper.toGenericRecord should adhere `csv.separator`") {
    val rows =
      """rollulus| rouloul|male| true |1998|1.96
        |felix |domesticus|female|false|1888|.23
        |""".stripMargin.split("\n").toSeq

    verify(
      """csv.columns=firstName|||awesome||length
        |csv.separator=\\|
        |""".stripMargin, schema, rows, expectedFields)
  }

}