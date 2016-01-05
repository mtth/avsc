#!/usr/bin/env python
# encoding: utf-8

"""Analyze timings data using pandas.

Usage:
  analyze.py [-c] PATH

Arguments:
  PATH            Path to JSON file containing timings data.

Options:
  -c              Output chart.
  -h              Show this message and exit.

Example:
  $ python analyze.py timings.json

  command:

            lib1          lib2
  schema    ops     %     ops     %
  schema1   123     1.0   100     0.72
  schema2   89      0.3   300     1.0

"""

from docopt import docopt
from json import load
import matplotlib.pyplot as plt
import pandas as pd
import sys


pd.set_option('display.max_columns', 20)
pd.set_option('expand_frame_repr', False)

def get_df(path):
  """Load raw dataframe from JSON data."""
  with open(path) as reader:
    df = pd.DataFrame(load(reader))
  df['rate'] = 1e3 / df['ms_per_record']
  return df

def get_ops_df(df):
  """Get dataframe of operations per second."""
  df = df.groupby(['schema', 'library'])['rate'].median()
  udf = df.unstack()
  stacked = {}
  for name, row in udf.iterrows():
    schema_df = row.to_frame('ops')
    max_rate = schema_df['ops'].max()
    schema_df['%'] = 100 * schema_df['ops'] / max_rate
    schema_df = schema_df.fillna(-1).applymap(round)
    stacked[name] = schema_df.stack()
  fdf = pd.DataFrame(stacked).transpose()
  fdf.index.name = 'schema'
  return fdf

def plot(df, command, schema, libraries=None, axes=None):
  filtered = df[df['schema'] == schema][df['command'] == command]
  grouped = filtered.groupby(['library'])
  rates = grouped['rate'].median()
  if libraries:
    rates = rates[libraries]
  rates = rates.transpose()
  ax = rates.plot(
    kind='bar',
    color=['steelblue', 'grey', 'grey', 'grey', 'grey'],
    ax=axes
  )
  ax.tick_params(axis='x', which='both', bottom='off', top='off')
  ax.tick_params(axis='y', which='both', left='off', right='off')
  ax.spines['top'].set_visible(False)
  ax.spines['right'].set_visible(False)
  ax.yaxis.grid(True)
  ax.set_xticklabels(rates.index, rotation=0)
  ax.set_xlabel('')
  ax.set_ylabel('Throughput (records per second)')
  return ax

if __name__ == '__main__':
  args = docopt(__doc__)
  DF = get_df(args['PATH'])
  for name, df in DF.groupby('command'):
    print '%s\n\n%s\n' % (name, get_ops_df(df))
  if args['-c']:
    libraries = ['node-avsc', 'node-json', 'node-protobuf', 'node-pson', 'node-msgpack']
    fig, axes = plt.subplots(nrows=2, ncols=1)
    ax1 = plot(DF, 'decode', 'Coupon.avsc', libraries=libraries, axes=axes[0])
    ax1.set_title('Decoding')
    ax2 = plot(DF, 'encode', 'Coupon.avsc', libraries=libraries, axes=axes[1])
    ax2.set_title('Encoding')
    plt.show()
