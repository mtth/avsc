#!/usr/bin/env node

'use strict';

var avsc = require('../../../../lib'),
    pson = require('pson');

var dataPath = process.argv[2];
if (!dataPath) {
  process.exit(1);
}

var loops = 3;
var records = [];
var pPair = new pson.ProgressivePair([]);
var sPair;

avsc.createFileDecoder(dataPath)
  .on('data', function (record) {
    // Learn data upfront.
    pPair.include(record);
    records.push(record);
  })
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
    console.log(1000 * (time[0] + time[1] * 1e-9) / (records.length * loops));
  });


function loop() {
  var n = 0;
  var i, l, buf;
  for (i = 0, l = records.length; i < l; i++) {
    buf = sPair.encode(records[i]).toBuffer();
    n += buf[0] + buf.length;
  }
  return n;
}
