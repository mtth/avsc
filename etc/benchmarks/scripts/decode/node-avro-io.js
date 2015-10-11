#!/usr/bin/env node

/* jshint node: true */

'use strict';

var io = require('node-avro-io'),
    avsc = require('../../../../lib');


var loops = 2;
var bufs = [];
var reader;

avsc.createFileDecoder(process.argv[2])
  .on('metadata', function (type) {
    var schema = new io.Schema.Schema(JSON.parse(type.toString()));
    reader = new io.IO.DatumReader(schema, schema);
  })
  .on('data', function (record) { bufs.push(record.$toBuffer()); })
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
    console.log(1000 * (time[0] + time[1] * 1e-9) / (bufs.length * loops));
  });

function deserialize(buffer) {
  if (!Buffer.isBuffer(buffer)) {
    throw "Buffer object expected";
  }

  var decoder = new io.IO.BinaryDecoder({
    _i: 0,
    read: function(len) {
      if (this._i + len > buffer.length) {
        throw "reading after buffer exhausted"
      }
      var i = this._i;
      this._i += len;
      return len == 1
        ? buffer[i]
        : buffer.slice(i, this._i);
    },
    skip: function(len) {
      if (this._i + len > buffer.length) {
        throw "reading after buffer exhausted"
      }
      this._i += len;
    }
  });
  return reader.read(decoder);
}

function loop() {
  var n = 0;
  var i, l, record;
  for (i = 0, l = bufs.length; i < l; i++) {
    record = deserialize(bufs[i]);
    if (record.$ !== null) {
      n++;
    }
  }
  return n;
}
