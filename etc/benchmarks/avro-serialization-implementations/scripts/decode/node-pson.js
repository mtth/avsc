#!/usr/bin/env node

'use strict';

let avsc = require('../../../../lib'),
    pson = require('pson');

let dataPath = process.argv[2];
if (!dataPath) {
  process.exit(1);
}

let loops = 3;
let bufs = [];
let pPair = new pson.ProgressivePair([]);
let sPair;

avsc.createFileDecoder(dataPath)
  .on('data', (record) => { bufs.push(pPair.toBuffer(record)); })
  .on('end', () => {
    let n = 0;
    sPair = new pson.StaticPair(pPair.decoder.dict);
    let time = process.hrtime();
    for (let i = 0; i < loops; i++) {
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
  for (let i = 0, l = bufs.length; i < l; i++) {
    let record = sPair.decode(bufs[i]);
    if (record.$ !== null) {
      n++;
    }
  }
  return n;
}
