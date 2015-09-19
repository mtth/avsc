/* jshint node: true */

'use strict';

var avsc = require('../lib'),
    fs = require('fs');


/**
 * Fast way to count records contained in an Avro container file.
 *
 * @param input {Stream} Input stream from an Avro container file.
 * @param cb(err, nRecords) {Function} Callback.
 *
 */
function countRecords(input, cb) {

  var n = 0;
  input
    .pipe(new avsc.streams.ContainerDecoder())
    .on('data', function (block) { n += block.count; })
    .on('end', function () { cb(null, n); })
    .on('error', cb);

}

// Driver.
var path = process.argv[2];
if (!path) {
  console.error('usage: node count.js PATH');
  process.exit(1);
}
countRecords(fs.createReadStream(path), function (err, nRecords) {
  if (err) {
    console.error(err);
    process.exit(1);
  }
  console.log(nRecords + '\t' + path);
});
