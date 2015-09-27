#!/usr/bin/env python
# encoding: utf-8

"""Benchmark driver.

All benchmarks are run sequentially as separate processes. They must output the
effective time spent processing or return a non-zero status code on error.

"""

from contextlib import contextmanager
from subprocess import PIPE, Popen
from tempfile import mkstemp
import logging as lg
import os
import os.path as osp
import sys


_logger = lg.getLogger(__name__)
lg.basicConfig(level=lg.INFO)

DPATH = osp.dirname(__file__)

SCHEMAS = [
  (fname, osp.join(DPATH, 'schemas', fname))
  for fname in os.listdir(osp.join(DPATH, 'schemas'))
]

os.environ['AVSC_JAVA_SCRIPTS'] = osp.join(DPATH, 'tools', 'scripts.jar')

@contextmanager
def temppath(dpath=None):
  """Create a temporary path."""
  (desc, path) = mkstemp(dir=dpath)
  os.close(desc)
  os.remove(path)
  try:
    _logger.debug('Created temporary path at %s.', path)
    yield path
  finally:
    if osp.exists(path):
      if osp.isdir(path):
        rmtree(path)
        _logger.debug('Deleted temporary directory at %s.', path)
      else:
        os.remove(path)
        _logger.debug('Deleted temporary file at %s.', path)
    else:
      _logger.debug('No temporary file or directory to delete at %s.', path)

def run(args):
  """Run a command in a separate process."""
  process = Popen(args, stdout=PIPE, stderr=PIPE)
  process.wait()
  if process.returncode:
    return None
  return process.stdout.read().strip()

def generate_data(schema_path, n_records, output_path, codec='deflate'):
  """Generate fake Avro records using Avro tools."""
  run([
    'java', '-jar', osp.join(DPATH, 'tools', 'avro-tools.jar'),
    'random', '--count', str(n_records), '--codec', codec,
    '--schema-file', schema_path, output_path
  ])

def run_benchmark(dname, count=100000):
  """Run all scripts inside a given benchmark folder."""
  print dname
  dpath = osp.join(DPATH, 'scripts', dname)
  scripts = [(fname, osp.join(dpath, fname)) for fname in os.listdir(dpath)]
  for schema_name, schema_path in SCHEMAS:
    print schema_name
    with temppath() as tpath:
      generate_data(schema_path, count, tpath)
      for name, path in scripts:
        out = run([path, schema_path, tpath])
        if out:
          ms = float(out) # Ms per record.
          throughput = int(1e3 / ms)
          print '%i\t%f\t%s' % (throughput, ms, name)

run_benchmark('encode', 10000)
