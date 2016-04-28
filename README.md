Generic Csv to Avro mapper
==========================

[![Build Status](https://travis-ci.org/rollulus/kafka-streams-csv2avro.svg?branch=master)](https://travis-ci.org/rollulus/kafka-streams-csv2avro)

Maps csv records to Avro records.

Proof of concept.
Work in progress. 

Input
-----

A stream with String values, each String is a csv record.

Output
------

A stream with Avro records. The fields of the record correspond to the csv values.

Quick Example
-------------

For example, provided that this Avro schema is given in the configuration:

```json
{"type": "record", "name": "Person", "fields" : [
    {"name": "firstName", "type": "string"},
    {"name": "lastName", "type": "string"},
    {"name": "length", "type": "float"},
    {"name": "awesome", "type": "boolean"}]}
```

And that the .csv layout is described like:

```properties
csv.separator=,
csv.columns=awesome,length,,firstName,lastName
```

Then the stream processor will map inputs like this:

    true,1.96,male,Rollulus,Rouloul

To an Avro record whose JSON representation is:

    {"firstName":"Rollulus","lastName":"Rouloul","length":1.96,"awesome":true}

