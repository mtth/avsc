#!/usr/bin/env python
# encoding: utf-8

"""Avsc benchmark runner.

Usage:
  run.py [-c COMMANDS] [-n ITERATIONS] [-r RECORDS] [-s SCHEMAS] [LIB ...]
  run.py -h

Arguments:
  LIB             Library to run. E.g. `java-avro`, `node-avsc`.

Options:
  -c COMMANDS     Commands to run. All if unspecified.
  -n ITERATIONS   Number of iterations. [default: 10]
  -r RECORDS      Number of random records generated. [default: 100000]
  -s SCHEMAS      Comma-separated list of schemas to test. All if unspecified.
  -h              Show this message and exit.

Examples:
  python run.py >timings.json
  python run.py -n 5 -s ArrayString node-avsc

Outputs a JSON file of timings.

"""

from contextlib import contextmanager
from docopt import docopt
from json import dumps
from subprocess import PIPE, Popen, call
from tempfile import mkstemp
import logging as lg
import os
import os.path as osp
import sys


DPATH = osp.dirname(osp.dirname(__file__))
FORMAT = '%(asctime)s %(levelname)s %(message)s'

_logger = lg.getLogger(__name__)
lg.basicConfig(level=lg.INFO, format=FORMAT)


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

  _schemas_dpath = osp.join(DPATH, os.pardir, os.pardir, 'schemas')
  _scripts_dpath = osp.join(DPATH, 'scripts')

  def __init__(self, name, n_records, attempts, libs, commands):
    _logger.info('starting benchmark for %s [%s records]', name, n_records)
    self.name = name
    self.path = osp.join(self._schemas_dpath, name)
    if not osp.exists(self.path):
      raise ValueError('no schema named %s' % (name, ))
    self.n_records = n_records
    self.attempts = attempts
    self.libs = libs
    self.commands = sorted(commands or os.listdir(self._scripts_dpath))

  def run(self):
    """Return list of timings."""
    times = []
    for attempt in range(self.attempts):
      with self._generate_data() as tpath:
        for dname in self.commands:
          dpath = osp.join(self._scripts_dpath, dname)
          for fname in os.listdir(dpath):
            if self.libs and not osp.splitext(fname)[0] in self.libs:
              _logger.info('skipped %s %s', dname, fname)
            else:
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
                  'n_records': self.n_records,
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
        'node', osp.join(DPATH, os.pardir, os.pardir, 'scripts', 'random'),
        self.path, str(self.n_records), path
      ])
      yield path
    finally:
      if osp.exists(path):
        os.remove(path)

  @classmethod
  def run_all(cls, libs, commands, fnames=None, n_records=10000, attempts=5):
    """Run all benchmarks."""
    times = []
    try:
      build_avsc_jar()
    except Exception:
      pass # Missing dependency, skip.
    available_names = set(os.listdir(cls._schemas_dpath))
    fnames = fnames or available_names
    for fname in sorted(fnames):
      if fname in available_names:
        bench = Benchmark(fname, n_records, attempts, libs, commands)
        times.extend(bench.run())
      else:
        _logger.warn('schema %s not found', fname)
    return times

if __name__ == '__main__':
  args = docopt(__doc__)
  if args['-s']:
    fnames = ['%s.avsc' % (elem, ) for elem in args['-s'].split(',')]
  else:
    fnames = []
  commands = args['-c'].split(',') if args['-c'] else None
  TIMES = Benchmark.run_all(
    libs=set(args['LIB']),
    commands=commands,
    fnames=fnames,
    n_records=int(args['-r']),
    attempts=int(args['-n']),
  )
  print dumps(TIMES)
