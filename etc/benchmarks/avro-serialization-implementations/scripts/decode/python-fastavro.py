#!/usr/bin/env python2.7
# encoding: utf-8

"""Fastavro decoding benchmark."""

from io import BytesIO
from itertools import repeat
from time import time
from fastavro import dump, load, acquaint_schema, reader as avro_reader
import sys


LOOPS = 2

with open(sys.argv[1]) as reader:
  records = avro_reader(reader)
  SCHEMA = records.schema
  BUFS = []
  for record in records:
    buf = BytesIO()
    dump(buf, record, SCHEMA)
    BUFS.append(buf)

start = time()
n = 0
for _ in repeat(None, LOOPS):
  for buf in BUFS:
    n += 1
    buf.seek(0)
    record = load(buf, SCHEMA)
print 1000. * (time() - start) / n
