#!/usr/bin/env node

'use strict';

let avro = require('etp-avro'),
    avsc = require('../../../../lib');


let loops = 2;
let records = [];
let cache, schema, writer;

avsc.createFileDecoder(process.argv[2])
  .on('metadata', (type) => { schema = JSON.parse(type.toString()); })
  .on('data', (record) => { records.push(record); })
  .on('end', () => {
    let n = 0;
    let time = process.hrtime();
    cache = new avro.SchemaCache([]);
    writer = new avro.BinaryWriter(cache);
    for (let i = 0; i < loops; i++) {
      n += loop();
    }
    time = process.hrtime(time);
    if (n <= 0) {
      throw new Error('no');
    }
    console.log(1000 * (time[0] + time[1] * 1e-9) / (records.length * loops));
  });

function loop() {
  let n = 0;
  for (let i = 0, l = records.length; i < l; i++) {
    // We need to slice to force a copy otherwise the array is shared.
    let buf = writer.encode(schema, records[i]).slice();
    n += buf[0] + buf.length;
  }
  return n;
}
