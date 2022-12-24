#!/usr/bin/env node

'use strict';

let io = require('node-avro-io'),
    avsc = require('../../../../lib'),
    {isBufferLike} = require('../../../../util');


let loops = 2;
let records = [];
let writer;

avsc.createFileDecoder(process.argv[2])
  .on('metadata', (type) => {
    let schema = new io.Schema.Schema(JSON.parse(type.toString()));
    writer = new io.IO.DatumWriter(schema);
  })
  .on('data', (record) => { records.push(record); })
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
    console.log(1000 * (time[0] + time[1] * 1e-9) / (records.length * loops));
  });

function serialize(datum) {
  let buffer = Buffer.from([]);
  let encoder = new io.IO.BinaryEncoder({
    write: function(data) {
      if (!isBufferLike(data)) {
        data = Buffer.from([data]);
      }
      buffer = Buffer.concat([buffer, data]);
    }
  });
  writer.write(datum, encoder);
  return buffer;
}

function loop() {
  let n = 0;
  for (let i = 0, l = records.length; i < l; i++) {
    let buf = serialize(records[i]);
    n += buf[0] + buf.length;
  }
  return n;
}
