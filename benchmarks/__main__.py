#!/usr/bin/env python
# encoding: utf-8

"""Benchmark driver.

All benchmarks are run sequentially as separate processes. They must output the
effective time spent processing or return a non-zero status code on error.

"""

from contextlib import contextmanager
from json import dumps
from subprocess import PIPE, Popen, call
from tempfile import mkstemp
import logging as lg
import os
import os.path as osp
import sys


_logger = lg.getLogger(__name__)
lg.basicConfig(level=lg.INFO)

DPATH = osp.dirname(__file__)


def build_avsc_jar():
  """Also set `AVSC_JAR` environment variable appropriately."""
  jar_path = osp.join(DPATH, 'deps', 'avro', 'target', 'avsc.jar')
  if not osp.exists(jar_path):
    _logger.info('building avsc jar')
    pom_path = osp.join(DPATH, 'deps', 'avro', 'pom.xml')
    code = call(['mvn', '-f', pom_path, 'clean', 'compile', 'assembly:single'])
    if code:
      _logger.error('unable to build avsc jar')
      sys.exit(1)
  os.environ['AVSC_JAR'] = jar_path


class Benchmark(object):

  _schemas_dpath = osp.join(DPATH, 'schemas')
  _scripts_dpath = osp.join(DPATH, 'scripts')

  def __init__(self, name, n_records, attempts):
    _logger.info('starting benchmark for %s [%s records]', name, n_records)
    self.name = name
    self.path = osp.join(self._schemas_dpath, name)
    self.n_records = n_records
    self.attempts = attempts

  def run(self):
    """Return list of timings."""
    times = []
    for attempt in range(self.attempts):
      with self._generate_data() as tpath:
        for dname in os.listdir(self._scripts_dpath):
          dpath = osp.join(self._scripts_dpath, dname)
          for fname in os.listdir(dpath):
            fpath = osp.join(dpath, fname)
            process = Popen([fpath, tpath], stdout=PIPE)
            process.wait()
            if process.returncode:
              _logger.warn('error running %s %s', dname, fname)
            else:
              times.append({
                'attempt': attempt,
                'schema': self.name,
                'command': dname,
                'library': osp.splitext(fname)[0],
                'ms_per_record': float(process.stdout.read())
              })
              _logger.info('finished %s %s', dname, fname)
    return times

  @contextmanager
  def _generate_data(self, codec='deflate'):
    """Generate fake Avro records using Avro tools."""
    _logger.info('generating fake data')
    (desc, path) = mkstemp()
    os.close(desc)
    os.remove(path)
    try:
      call([
        'node', osp.join(DPATH, 'tools', 'random.js'),
        self.path, str(self.n_records), path
      ])
      yield path
    finally:
      if osp.exists(path):
        os.remove(path)

  @classmethod
  def run_all(cls, n_records=10000, attempts=5):
    """Run all benchmarks."""
    times = []
    build_avsc_jar()
    for fname in os.listdir(cls._schemas_dpath):
      bench = Benchmark(fname, n_records, attempts)
      times.extend(bench.run())
    return times

TIMES = Benchmark.run_all(50000, 10)
print dumps(TIMES)
