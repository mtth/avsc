#!/usr/bin/env python2.7
# encoding: utf-8

"""Python avro official implementation decoding benchmark."""

from io import BytesIO
from itertools import repeat
from time import time
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter, BinaryEncoder, BinaryDecoder
import sys


LOOPS = 1

with open(sys.argv[1]) as reader:
  datum_reader = DatumReader()
  file_reader = DataFileReader(reader, datum_reader)
  SCHEMA = datum_reader.writers_schema
  BUFS = []
  datum_writer = DatumWriter(SCHEMA)
  for record in file_reader:
    buf = BytesIO()
    encoder = BinaryEncoder(buf)
    datum_writer.write(record, encoder)
    BUFS.append(buf)

datum_reader = DatumReader(SCHEMA)
start = time()
n = 0
for _ in repeat(None, LOOPS):
  for buf in BUFS:
    n += 1
    buf.seek(0)
    record = datum_reader.read(BinaryDecoder(buf))
print 1000. * (time() - start) / n
