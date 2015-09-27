#!/usr/bin/env python
# encoding: utf-8

"""Analyze timings data using pandas."""

from json import load
import pandas as pd
import sys

def get_rate_df(df, command):
  """Output:

            lib1          lib2
            abs     rel   abs     rel
  schema1   123     1.0   100     0.72
  schema2   89      0.3   300     1.0

  """
  df = df[df['command'] == command]
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
  return pd.DataFrame(stacked).transpose()

def main(path):
  with open(path) as reader:
    df = pd.DataFrame(load(reader))
  df['rate'] = 1e3 / df['ms_per_record']
  # Encoding
  return get_rate_df(df, 'decode')
  # Decoding

if __name__ == '__main__':
  DF = main(sys.argv[1])
