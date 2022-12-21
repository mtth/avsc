#!/usr/bin/env node

'use strict';

let avsc = require('../../../../lib'),
    msgpack = require('msgpack-lite');

let dataPath = process.argv[2];
if (!dataPath) {
  process.exit(1);
}

let loops = 3;
let records = [];

avsc.createFileDecoder(dataPath)
  .on('data', (record) => { records.push(record); })
  .on('end', () => {
    let i = 0;
    let n = 0;
    let time = process.hrtime();
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
  let n = 0;
  let i, l, buf;
  for (i = 0, l = records.length; i < l; i++) {
    buf = msgpack.encode(records[i]);
    n += buf[0] + buf.length;
  }
  return n;
}
