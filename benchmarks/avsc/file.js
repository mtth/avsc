/* jshint node: true */

'use strict';

var streams = require('../../lib/streams'),
    fs = require('fs'),
    util = require('util');

// Buffer.prototype.toJSON = function () { return this.toString('binary'); };

var n1 = 0;
var n2 = 0;
var time;
fs.createReadStream('dat/user-5000000.avro')
  .pipe(new streams.Decoder())
  .on('metadata', function (schema, codec, sync) {
    // console.log(schema);
    // console.log(codec);
    // console.log(sync);
    time = process.hrtime();
  })
  // .on('data', function () {})
  .on('data', function (record) {
    n1++;
    if (record.name === null) {
      this.emit('error', new Error('no name'));
    }
  })
  // .on('block', function (n) { n2 += n; })
  .once('end', function () {
    time = process.hrtime(time);
    console.log(util.format('%s\t%s\t%s', n1, n2,  time[0] + time[1] * 1e-9));
  });
