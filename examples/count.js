/* jshint node: true */

'use strict';

/**
 * Example of decoding an Avro file.
 *
 * When the file is an object container file, this is suboptimal since we could
 * just use the count from each block without decoding its contents.
 *
 */

var avsc = require('../lib'),
    util = require('util');


var path = process.argv[2];
if (!path) {
  console.error('usage: node count.js PATH');
  process.exit(1);
}

var n = 0;
var time;
avsc.decodeFile(path)
  .on('metadata', function () { time = process.hrtime(); })
  .on('data', function () { n++; })
  .once('end', function () {
    time = process.hrtime(time);
    console.log(util.format(
      'counted %s records in %s seconds',
      n,  time[0] + time[1] * 1e-9
    ));
  });
