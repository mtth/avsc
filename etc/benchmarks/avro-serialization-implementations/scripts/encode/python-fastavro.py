#!/usr/bin/env python2.7
# encoding: utf-8

"""Fastavro."""

from io import BytesIO
from itertools import repeat
from time import time
from fastavro import dump, load, acquaint_schema, reader as avro_reader
import sys

LOOPS = 2

with open(sys.argv[1]) as reader:
  records = avro_reader(reader)
  SCHEMA = records.schema
  RECORDS = list(records)

buf = BytesIO()
m = 0
n = 0
start = time()
for _ in repeat(None, LOOPS):
  for record in RECORDS:
    dump(buf, record, SCHEMA)
    m += buf.tell()
    n += 1
    buf.seek(0)
if m <= 0:
  raise Exception('no')
print 1000. * (time() - start) / n
