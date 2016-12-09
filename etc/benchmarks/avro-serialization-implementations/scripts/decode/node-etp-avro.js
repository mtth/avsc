#!/usr/bin/env node

/* jshint node: true */

'use strict';

var avro = require('etp-avro'),
    avsc = require('../../../../lib');


var loops = 2;
var bufs = [];
var cache, reader, schema;

avsc.createFileDecoder(process.argv[2])
  .on('metadata', function (type) { schema = JSON.parse(type.toString()); })
  .on('data', function (record) { bufs.push(record.$toBuffer()); })
  .on('end', function () {
    var i = 0;
    var n = 0;
    var time = process.hrtime();
    cache = new avro.SchemaCache([]);
    reader = new avro.BinaryReader();
    for (i = 0; i < loops; i++) {
      n += loop();
    }
    time = process.hrtime(time);
    if (n <= 0) {
      throw new Error('no');
    }
    console.log(1000 * (time[0] + time[1] * 1e-9) / (bufs.length * loops));
  });

function loop() {
  var n = 0;
  var i, l, record;
  for (i = 0, l = bufs.length; i < l; i++) {
    record = reader.decode(schema, bufs[i]);
    if (record.$ !== null) {
      n++;
    }
  }
  return n;
}
