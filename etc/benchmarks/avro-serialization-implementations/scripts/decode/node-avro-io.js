#!/usr/bin/env node

'use strict';

let io = require('node-avro-io'),
    avsc = require('../../../../lib');


let loops = 2;
let bufs = [];
let reader;

avsc.createFileDecoder(process.argv[2])
  .on('metadata', (type) => {
    let schema = new io.Schema.Schema(JSON.parse(type.toString()));
    reader = new io.IO.DatumReader(schema, schema);
  })
  .on('data', (record) => { bufs.push(record.$toBuffer()); })
  .on('end', () => {
    let n = 0;
    let time = process.hrtime();
    for (let i = 0; i < loops; i++) {
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
    throw 'Buffer object expected';
  }

  let decoder = new io.IO.BinaryDecoder({
    _i: 0,
    read: function(len) {
      if (this._i + len > buffer.length) {
        throw 'reading after buffer exhausted';
      }
      let i = this._i;
      this._i += len;
      return len == 1 ?
        buffer[i] :
        buffer.subarray(i, this._i);
    },
    skip: function(len) {
      if (this._i + len > buffer.length) {
        throw 'reading after buffer exhausted';
      }
      this._i += len;
    }
  });
  return reader.read(decoder);
}

function loop() {
  let n = 0;
  for (let i = 0, l = bufs.length; i < l; i++) {
    let record = deserialize(bufs[i]);
    if (record.$ !== null) {
      n++;
    }
  }
  return n;
}
