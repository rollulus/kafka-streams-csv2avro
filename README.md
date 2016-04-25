Generic Csv to Avro mapper
==========================

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

The stream processor will map an input like this:

    Rollulus, Rouloul, 1.96, true

To an Avro record whose JSON representation is:

    {"firstName":"Rollulus","lastName":"Rouloul","length":1.96,"awesome":true}

