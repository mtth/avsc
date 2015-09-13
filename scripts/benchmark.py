#!/usr/bin/env python
# encoding: utf-8

"""Fastavro."""

from fastavro import acquaint_schema, schemaless_reader
from io import BytesIO
from itertools import repeat
from json import load
from time import time

with open('dat/event.avsc') as reader:
  schema = load(reader)

acquaint_schema(schema)

with open('dat/event.avro') as reader:
  buf = BytesIO(reader.read())

start = time()
n = 100000
for _ in repeat(None, n):
  record = schemaless_reader(buf, schema)
  assert record['header']['memberId']
  buf.seek(0)
print 1000. * (time() - start) / n
