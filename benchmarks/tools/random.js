/* jshint node: true */

'use strict';

/**
 * Generate fake data from a schema, suitable to run benchmarks.
 *
 * This is very similar to the `random` command of Avro tools.
 *
 */

var avsc = require('../../lib'),
    fs = require('fs');

var schemaPath = process.argv[2];
var count = Number.parseInt(process.argv[3]);
var filePath = process.argv[4];
if (!filePath) {
  console.error('usage: node random.js SCHEMA COUNT OUT');
  process.exit(1);
}

var type = avsc.parse(schemaPath);
var encoder = new avsc.streams.BlockEncoder(type);
encoder.pipe(fs.createWriteStream(filePath, {defaultEncoding: 'binary'}));
console.log(type.toString());

while (count--) {
  encoder.write(type.random());
}
encoder.end();
