#!/usr/bin/env python2.7
# encoding: utf-8

"""Python avro official implementation encoding benchmark."""

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
  RECORDS = list(file_reader)

buf = BytesIO()
datum_writer = DatumWriter(SCHEMA)
start = time()
n = 0
for _ in repeat(None, LOOPS):
  for record in RECORDS:
    buf.seek(0)
    encoder = BinaryEncoder(buf)
    datum_writer.write(record, encoder)
    n += 1
print 1000. * (time() - start) / n
