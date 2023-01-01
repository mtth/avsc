#!/usr/bin/env node

'use strict';

/**
 * Get various performance rates.
 *
 * Usage:
 *
 *   ./perf [-w] SCHEMA ...
 *
 * Options:
 *
 *   -w     Use wrapped unions.
 *
 */

var avro = require('../../lib'),
    Benchmark = require('benchmark'),
    path = require('path');


var paths = process.argv.slice(2);
var index = paths.indexOf('-w');
var wrap = false;
if (~index) {
  paths.splice(index, 1);
  wrap = true;
}

// Number of random values to generate to try per schema. This is mostly to
// reduce the variability introduced by unions (which can greatly affect
// serialization speed).
var NUM_VALUES = 1000;

// Header formatting is done according to GitHub flavored Markdown.
console.log(['fromBuffer', 'toBuffer', 'isValid ', '(ops/sec)'].join('\t| '));
console.log(['---------:', '-------:', '------: ', '---------'].join('\t| '));

paths.forEach(function (fpath) {
  var type = avro.parse(fpath, {wrapUnions: wrap});
  var values = [];
  var bufs = [];

  var i, l, val;
  for (i = 0, l = NUM_VALUES; i < l; i++) {
    val = type.random();
    values.push(val);
    bufs.push(type.toBuffer(val));
  }

  var stats = [];
  var bench = new Benchmark().on('complete', function () {
    var s = '' + (NUM_VALUES * this.hz | 0);
    s = s.replace(/(?=(?:\d{3})+$)(?!\b)/g, ',');
    s = (s + '            ').slice(0, 12);
    stats.push(s);
  });

  bench.clone({fn: function () {
    var i, l, val;
    for (i = 0, l = NUM_VALUES; i < l; i++) {
      val = type.fromBuffer(bufs[i]);
      if (val.$) {
        throw new Error();
      }
    }
  }}).run();

  bench.clone({fn: function () {
    var i, l, buf;
    for (i = 0, l = NUM_VALUES; i < l; i++) {
      buf = type.toBuffer(values[i]);
      if (!buf.length) {
        throw new Error();
      }
    }
  }}).run();

  bench.clone({fn: function () {
    var i, l;
    for (i = 0, l = NUM_VALUES; i < l; i++) {
      if (!type.isValid(values[i])) {
        throw new Error();
      }
    }
  }}).run();

  stats.push(path.basename(fpath));
  console.log(stats.join('\t| '));
});
