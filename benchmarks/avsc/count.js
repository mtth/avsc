/* jshint node: true */

'use strict';

var file = require('../../lib/file'),
    fs = require('fs'),
    util = require('util');

var input = fs.createReadStream('benchmarks/dat/user-100000.avro');
var time = process.hrtime();
file.countRecords(input, function (err, n) {
  time = process.hrtime(time);
  if (err) {
    console.error(err);
    return;
  }
  console.log(util.format('%s %s', n, time[0] + time[1] * 1e-9));
});
