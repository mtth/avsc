#!/usr/bin/env node

'use strict';

/**
 * Generate fake data from a schema, suitable to run benchmarks.
 *
 * This is very similar to the `random` command of Avro tools.
 *
 */

let avro = require('../../lib'),
    assert = require('assert'),
    fs = require('fs');

let schemaPath = process.argv[2];
let count = Number.parseInt(process.argv[3]);
let filePath = process.argv[4];
if (!filePath) {
  console.error(`usage: ${process.argv[1]} SCHEMA COUNT PATH`);
  process.exit(1);
}

fs.readFile(schemaPath, {encoding: 'utf8'}, (err, str) => {
  assert(!err, err);
  let type = avro.Type.forSchema(JSON.parse(str));
  let encoder = avro.createFileEncoder(filePath, type);
  while (count--) {
    encoder.write(type.random());
  }
  encoder.end();
});
