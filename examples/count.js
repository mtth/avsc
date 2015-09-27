/* jshint node: true */

'use strict';

/**
 * Example of decoding an Avro file.
 *
 * When the file is an object container file, this is suboptimal since we could
 * just use the count from each block without decoding its contents. Using a
 * reader schema would also help here.
 *
 */

var avsc = require('../lib'),
    util = require('util');


var schema = process.argv[2];
var path = process.argv[3];
var type;
if (path === undefined) {
  path = schema;
  type = undefined;
} else {
  type = avsc.parse(JSON.parse(schema));
}

if (!path) {
  console.error('usage: node count.js [SCHEMA] PATH');
  process.exit(1);
}

var n = 0;
var time;
avsc.decodeFile(path, {type: type, decode: false})
  .on('data', function () {
    if (!time) {
      // In the case of Avro container files, we could use the 'metadata' event
      // to set the time instead.
      time = process.hrtime();
    }
    n++;
  })
  .on('error', function (err) {
    console.error(err.message);
    process.exit(1);
  })
  .once('end', function () {
    time = process.hrtime(time);
    console.log(util.format(
      'counted %s records in %s seconds',
      n,  time[0] + time[1] * 1e-9
    ));
  });
