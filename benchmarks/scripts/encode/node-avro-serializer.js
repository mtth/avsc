#!/usr/bin/env node

/* jshint node: true */

'use strict';

var Serializer = require('avro-serializer'),
    avsc = require('avsc');


var records = [];
var serializer;

avsc.decodeFile(process.argv[3])
  .on('metadata', function (type) { serializer = new Serializer(type); })
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
    buf = serializer.serialize(records[i]);
    n += buf[0];
  }
  return n;
}
