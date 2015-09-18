/* jshint node: true, mocha: true */

'use strict';

var file = require('../lib/file'),
    // assert = require('assert'),
    fs = require('fs');

suite('file', function () {

  suite('Decoder', function () {

    test('simple', function () {

      fs.createReadStream('dat/users.avro')
        .pipe(new file.BlockDecoder())
        .on('metadata', function (codec, sync) {
          console.log('codec: ' + codec);
          console.log(sync);
        })
        .on('data', function () {
          console.log('got block');
        })
        .on('error', function (err) { console.error(err); })
        .pipe(new file.RecordDecoder())
        .on('data', function (record) {
          console.log(record);
        });

    });

  });

});
