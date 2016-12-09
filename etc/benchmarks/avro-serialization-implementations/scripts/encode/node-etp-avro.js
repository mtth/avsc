#!/usr/bin/env node

/* jshint node: true */

'use strict';

var avro = require('etp-avro'),
    avsc = require('../../../../lib');


var loops = 2;
var records = [];
var cache, schema, writer;

avsc.createFileDecoder(process.argv[2])
  .on('metadata', function (type) { schema = JSON.parse(type.toString()); })
  .on('data', function (record) { records.push(record); })
  .on('end', function () {
    var i = 0;
    var n = 0;
    var time = process.hrtime();
    cache = new avro.SchemaCache([]);
    writer = new avro.BinaryWriter(cache);
    for (i = 0; i < loops; i++) {
      n += loop();
    }
    time = process.hrtime(time);
    if (n <= 0) {
      throw new Error('no');
    }
    console.log(1000 * (time[0] + time[1] * 1e-9) / (records.length * loops));
  });

function loop() {
  var n = 0;
  var i, l, buf;
  for (i = 0, l = records.length; i < l; i++) {
    // We need to slice to force a copy otherwise the array is shared.
    buf = writer.encode(schema, records[i]).slice();
    n += buf[0] + buf.length;
  }
  return n;
}
