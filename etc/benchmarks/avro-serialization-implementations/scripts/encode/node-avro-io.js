#!/usr/bin/env node

/* jshint node: true */

'use strict';

var io = require('node-avro-io'),
    avsc = require('../../../../lib');


var loops = 2;
var records = [];
var writer;

avsc.createFileDecoder(process.argv[2])
  .on('metadata', function (type) {
    var schema = new io.Schema.Schema(JSON.parse(type.toString()));
    writer = new io.IO.DatumWriter(schema);
  })
  .on('data', function (record) { records.push(record); })
  .on('end', function () {
    var i = 0;
    var n = 0;
    var time = process.hrtime();
    for (i = 0; i < loops; i++) {
      n += loop();
    }
    time = process.hrtime(time);
    if (n <= 0) {
      throw new Error('no');
    }
    console.log(1000 * (time[0] + time[1] * 1e-9) / (records.length * loops));
  });

function serialize(datum) {
  var buffer = new Buffer([]);
  var encoder = new io.IO.BinaryEncoder({
    write: function(data) {
      if (!Buffer.isBuffer(data)) {
        data = new Buffer([data]);
      }
      buffer = Buffer.concat([buffer, data]);
    }
  });
  writer.write(datum, encoder);
  return buffer;
}

function loop() {
  var n = 0;
  var i, l, buf;
  for (i = 0, l = records.length; i < l; i++) {
    buf = serialize(records[i]);
    n += buf[0] + buf.length;
  }
  return n;
}
