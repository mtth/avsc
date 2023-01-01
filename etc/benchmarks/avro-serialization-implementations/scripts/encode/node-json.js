#!/usr/bin/env node

'use strict';

let avsc = require('../../../../lib');

let dataPath = process.argv[2];
if (!dataPath) {
  process.exit(1);
}

let loops = 5;
let records = [];

avsc.createFileDecoder(dataPath)
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


function loop() {
  let n = 0;
  for (let i = 0, l = records.length; i < l; i++) {
    let s = JSON.stringify(records[i]);
    n += s.charCodeAt(0) + s.length;
  }
  return n;
}
