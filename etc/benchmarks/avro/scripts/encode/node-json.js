#!/usr/bin/env node

'use strict';

var avsc = require('../../../../lib');

var dataPath = process.argv[2];
if (!dataPath) {
  process.exit(1);
}

var loops = 5;
var records = [];

avsc.createFileDecoder(dataPath)
  .on('data', function (record) { records.push(record); })
  .on('end', function () {
    var i = 0;
    var n = 0;
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
  var i, l, s;
  for (i = 0, l = records.length; i < l; i++) {
    s = JSON.stringify(records[i]);
    n += s.charCodeAt(0) + s.length;
  }
  return n;
}
