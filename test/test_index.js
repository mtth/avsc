/* jshint node: true, mocha: true */

'use strict';

var avsc = require('../lib'),
    assert = require('assert'),
    path = require('path');


var DPATH = path.join(__dirname, 'dat');


suite('index', function () {

  suite('parse', function () {

    test('object', function () {
      var obj = {
        type: 'record',
        name: 'Person',
        fields: [{name: 'so', type: 'Person'}]
      };
      assert(avsc.parse(obj) instanceof avsc.types.RecordType);
    });

    test('schema instance', function () {
      var type = avsc.parse({
        type: 'record',
        name: 'Person',
        fields: [{name: 'so', type: 'Person'}]
      });
      assert.strictEqual(avsc.parse(type), type);
    });

    test('stringified schema', function () {
      assert(avsc.parse('"int"') instanceof avsc.types.IntType);
    });

    test('type name', function () {
      assert(avsc.parse('double') instanceof avsc.types.DoubleType);
    });

    test('file', function () {
      var t1 = avsc.parse({type: 'fixed', name: 'id.Id', size: 64});
      var t2 = avsc.parse(path.join(DPATH, 'Id.avsc'));
      assert.deepEqual(JSON.stringify(t1), JSON.stringify(t2));
    });

  });

  suite('decode', function () {

    var type = avsc.parse(path.join(DPATH, 'Person.avsc'));

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

    test('getFileHeader', function () {
      var header;
      var fpath = path.join(DPATH, 'person-10.avro');
      header = avsc.getFileHeader(fpath);
      assert(header !== null);
      assert.equal(typeof header.meta['avro.schema'], 'object');
      header = avsc.getFileHeader(fpath, {decode: false});
      assert(Buffer.isBuffer(header.meta['avro.schema']));
      header = avsc.getFileHeader(fpath, {size: 2});
      assert.equal(typeof header.meta['avro.schema'], 'object');
      header = avsc.getFileHeader(path.join(DPATH, 'person-10.avro.raw'));
      assert(header === null);
      header = avsc.getFileHeader(path.join(DPATH, 'person-10.no-codec.avro'));
      assert(header !== null);
    });

  });

});
