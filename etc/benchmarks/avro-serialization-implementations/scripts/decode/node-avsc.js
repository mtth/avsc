#!/usr/bin/env node

'use strict';

let avsc = require('../../../../../lib');

let dataPath = process.argv[2];
if (!dataPath) {
  process.exit(1);
}

let loops = 5;
let bufs = [];
let type = null;

avsc.createFileDecoder(dataPath)
  .on('metadata', (writerType) => { type = writerType; })
  .on('data', (record) => { bufs.push(record.toBuffer()); })
  .on('end', () => {
    let n = 0;
    let time = process.hrtime();
    for (let i = 0; i < loops; i++) {
      n += loop();
    }
    time = process.hrtime(time);
    if (n < 0) {
      console.error('no');
    }
    console.log(1000 * (time[0] + time[1] * 1e-9) / (bufs.length * loops));
  });


function loop() {
  let n = 0;
  for (let i = 0, l = bufs.length; i < l; i++) {
    let record = type.fromBuffer(bufs[i]);
    if (record.$ !== null) {
      n++;
    }
  }
  return n;
}
