#!/usr/bin/env python
# encoding: utf-8

"""Fastavro."""

from itertools import repeat
from json import load
from time import time
import fastavro._reader as c_reader
import fastavro.reader as py_reader

def run(name, fn):
  with open('dat/user-100000.avro') as reader:
    reader = fn(reader)
    n = 0
    start = time()
    for record in reader:
      n += 1
  print '%s\t%s\t%s' % (name, n, time() - start)

run('c reader', c_reader.iter_avro)
run('py reader', py_reader.iter_avro)
