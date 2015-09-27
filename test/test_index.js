/* jshint node: true, mocha: true */

'use strict';

var avsc = require('../lib'),
    assert = require('assert'),
    path = require('path');


var DPATH = path.join(__dirname, 'dat');


suite('index', function () {

  suite('parse', function () {

    test('schema instance', function () {
      var type = avsc.parse({
        type: 'record',
        name: 'Person',
        fields: [{name: 'so', type: 'Person'}]
      });
      assert.strictEqual(avsc.parse(type), type);
    });

    test('file', function () {
      var t1 = avsc.parse({type: 'fixed', name: 'id.Id', size: 64});
      var t2 = avsc.parseFile(path.join(DPATH, 'Id.avsc'));
      assert.deepEqual(JSON.stringify(t1), JSON.stringify(t2));
    });

  });

  suite('decode', function () {

    var type = avsc.parseFile(path.join(DPATH, 'Person.avsc'));

    test('block file', function (cb) {
      var n = 0;
      var metadata = false;
      avsc.decodeFile(path.join(DPATH, 'person-10.avro'))
        .on('metadata', function (writerType) {
          assert.equal(type.toString(), writerType.toString());
          metadata = true;
        })
        .on('data', function (obj) {
          n++;
          assert(type.isValid(obj));
        })
        .on('end', function () {
          assert(metadata);
          assert.equal(n, 10);
          cb();
        });

    });

  });

});
