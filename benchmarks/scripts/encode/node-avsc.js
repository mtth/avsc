#!/usr/bin/env node

'use strict';

var avsc = require('../../../lib');

var dataPath = process.argv[3];
if (!dataPath) {
  process.exit(1);
}

var records = [];
var type = null;

avsc.decodeFile(dataPath)
  .on('metadata', function (writerType) { type = writerType; })
  .on('data', function (record) { records.push(record); })
  .on('end', function () {
    var i = 0;
    var n = 0;
    var loops = 50;
    var time = process.hrtime();
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
  var n = 0;
  var i, l, buf;
  for (i = 0, l = records.length; i < l; i++) {
    buf = type.encode(records[i], 3072, true);
    n += buf[0];
  }
  return n;
}