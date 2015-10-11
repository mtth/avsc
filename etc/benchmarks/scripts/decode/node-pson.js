#!/usr/bin/env node

'use strict';

var avsc = require('../../../../lib'),
    pson = require('pson');

var dataPath = process.argv[2];
if (!dataPath) {
  process.exit(1);
}

var loops = 3;
var bufs = [];
var pPair = new pson.ProgressivePair([]);
var sPair;

avsc.createFileDecoder(dataPath)
  .on('data', function (record) { bufs.push(pPair.toBuffer(record)); })
  .on('end', function () {
    var i = 0;
    var n = 0;
    sPair = new pson.StaticPair(pPair.decoder.dict);
    var time = process.hrtime();
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
  var n = 0;
  var i, l, record;
  for (i = 0, l = bufs.length; i < l; i++) {
    record = sPair.decode(bufs[i]);
    if (record.$ !== null) {
      n++;
    }
  }
  return n;
}
