#!/usr/bin/env node

'use strict';

/**
 * Generate fake data from a schema, suitable to run benchmarks.
 *
 * This is very similar to the `random` command of Avro tools.
 *
 */

var avro = require('../../lib'),
    assert = require('assert'),
    fs = require('fs'),
    util = require('util');

var schemaPath = process.argv[2];
var count = Number.parseInt(process.argv[3]);
var filePath = process.argv[4];
if (!filePath) {
  console.error(util.format('usage: %s SCHEMA COUNT PATH', process.argv[1]));
  process.exit(1);
}

fs.readFile(schemaPath, {encoding: 'utf8'}, function (err, str) {
  assert(!err, err);
  var type = avro.Type.forSchema(JSON.parse(str));
  var encoder = avro.createFileEncoder(filePath, type);
  while (count--) {
    encoder.write(type.random());
  }
  encoder.end();
});
