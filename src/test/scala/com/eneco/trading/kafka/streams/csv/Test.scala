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

  def verify(mapper: CsvToGenericRecordMapper, rows: Seq[String], expectedFields:Seq[Map[String, Any]]) = {
    val records = rows.map(mapper.toGenericRecord).map(_.get)
      .zip(expectedFields).foreach {
      case (rec, exp) => exp.foreach { case (k, v) => rec.get(k) shouldEqual v }
    }

  }

  test("ColumnNameDrivenMapper.toGenericRecord should map appropriately") {
    val columns = "firstName,,,awesome,,length"

    val rows =
      Seq("rollulus,rouloul,male,true,1998,1.96",
        "felix,domesticus,female,false,1888,.23")

    val expectedFields =
      Seq(Map("firstName" -> "rollulus", "length" -> Float.box(1.96f), "awesome" -> true),
        Map("firstName" -> "felix", "length" -> Float.box(0.23f), "awesome" -> false))

    verify(new ColumnNameDrivenMapper(schema, columns),
      rows, expectedFields)
  }

  test("ColumnNameDrivenMapper.toGenericRecord should handle non-comma separator") {
    val columns = "firstName|||awesome||length"

    val rows =
      Seq("rollulus|rouloul|male|true|1998|1.96",
        "felix|domesticus|female|false|1888|.23")

    val expectedFields =
      Seq(Map("firstName" -> "rollulus", "length" -> Float.box(1.96f), "awesome" -> true),
        Map("firstName" -> "felix", "length" -> Float.box(0.23f), "awesome" -> false))

    verify(new ColumnNameDrivenMapper(schema, columns, new RegexCsvTokenizer("\\|")),
      rows, expectedFields)
  }
}