#!/usr/bin/env python
# encoding: utf-8

"""Analyze timings data using pandas.

Usage:

  $ python analyze.py timings.json

  command:

            lib1          lib2
  schema    abs     rel   abs     rel
  schema1   123     1.0   100     0.72
  schema2   89      0.3   300     1.0

"""

from json import load
import pandas as pd
import sys


pd.set_option('display.max_columns', 20)
pd.set_option('expand_frame_repr', False)

def get_df(path):
  """Load raw dataframe."""
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
    schema_df = schema_df.fillna(-1)
    schema_df = schema_df.astype(int)
    stacked[name] = schema_df.stack()
  fdf = pd.DataFrame(stacked).transpose()
  fdf.index.name = 'schema'
  return fdf

if __name__ == '__main__':
  DF = get_df(sys.argv[1])
  for name, df in DF.groupby('command'):
    print '%s\n\n%s\n' % (name, get_ops_df(df, name))
