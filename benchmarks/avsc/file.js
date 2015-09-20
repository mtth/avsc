/* jshint node: true */

'use strict';

var avsc = require('../../lib'),
    util = require('util');

// Buffer.prototype.toJSON = function () { return this.toString('binary'); };

var n1 = 0;
var n2 = 0;
var time;
avsc.decodeFile('dat/user-5000000.avro')
  .on('metadata', function (schema, codec, sync) {
    console.log(util.format('%s\t%j', codec, sync));
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
