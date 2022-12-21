#!/usr/bin/env node

'use strict';

let avro = require('etp-avro'),
    avsc = require('../../../../lib');


let loops = 2;
let bufs = [];
let reader, schema;

avsc.createFileDecoder(process.argv[2])
  .on('metadata', (type) => { schema = JSON.parse(type.toString()); })
  .on('data', (record) => { bufs.push(record.$toBuffer()); })
  .on('end', () => {
    let n = 0;
    let time = process.hrtime();
    reader = new avro.BinaryReader();
    for (let i = 0; i < loops; i++) {
      n += loop();
    }
    time = process.hrtime(time);
    if (n <= 0) {
      throw new Error('no');
    }
    console.log(1000 * (time[0] + time[1] * 1e-9) / (bufs.length * loops));
  });

function loop() {
  let n = 0;
  for (let i = 0, l = bufs.length; i < l; i++) {
    let record = reader.decode(schema, bufs[i]);
    if (record.$ !== null) {
      n++;
    }
  }
  return n;
}
