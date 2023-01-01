#!/usr/bin/env node

'use strict';

let avsc = require('../../../../lib'),
    msgpack = require('msgpack-lite');

let dataPath = process.argv[2];
if (!dataPath) {
  process.exit(1);
}

let loops = 2;
let bufs = [];

avsc.createFileDecoder(dataPath)
  .on('data', (record) => { bufs.push(msgpack.encode(record)); })
  .on('end', () => {
    let i = 0;
    let n = 0;
    let time = process.hrtime();
    for (i = 0; i < loops; i++) {
      n += loop();
    }
    time = process.hrtime(time);
    if (n < 0) {
      throw new Error('no');
    }
    console.log(1000 * (time[0] + time[1] * 1e-9) / (bufs.length * loops));
  });


function loop() {
  let n = 0;
  let i, l, record;
  for (i = 0, l = bufs.length; i < l; i++) {
    record = msgpack.decode(bufs[i]);
    if (record.$ !== null) {
      n++;
    }
  }
  return n;
}
