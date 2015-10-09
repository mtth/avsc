/* jshint node: true, mocha: true */

'use strict';

var avsc = require('../lib'),
    assert = require('assert'),
    path = require('path');


var DPATH = path.join(__dirname, 'dat');


suite('index', function () {

  suite('createType', function () {

    test('object', function () {
      var obj = {
        type: 'record',
        name: 'Person',
        fields: [{name: 'so', type: 'Person'}]
      };
      assert(avsc.createType(obj) instanceof avsc.types.RecordType);
    });

    test('schema instance', function () {
      var type = avsc.createType({
        type: 'record',
        name: 'Person',
        fields: [{name: 'so', type: 'Person'}]
      });
      assert.strictEqual(avsc.createType(type), type);
    });

    test('stringified schema', function () {
      assert(avsc.createType('"int"') instanceof avsc.types.IntType);
    });

    test('type name', function () {
      assert(avsc.createType('double') instanceof avsc.types.DoubleType);
    });

    test('file', function () {
      var t1 = avsc.createType({type: 'fixed', name: 'id.Id', size: 64});
      var t2 = avsc.createType(path.join(DPATH, 'Id.avsc'));
      assert.deepEqual(JSON.stringify(t1), JSON.stringify(t2));
    });

  });

  suite('decode', function () {

    test('block file matching type', function (cb) {
      var n = 0;
      var type = avsc.createType(path.join(DPATH, 'Person.avsc'));
      avsc.createFileDecoder(path.join(DPATH, 'person-10.avro'), type)
        .on('data', function (obj) {
          n++;
          assert(type.isValid(obj));
        })
        .on('end', function () {
          assert.equal(n, 10);
          cb();
        });
    });

    test('block file no type', function (cb) {
      var n = 0;
      var type;
      avsc.createFileDecoder(path.join(DPATH, 'person-10.avro'))
        .on('metadata', function (writerType) { type = writerType; })
        .on('data', function (obj) {
          n++;
          assert(type.isValid(obj));
        })
        .on('end', function () {
          assert.equal(n, 10);
          cb();
        });
    });

    test('block file invalid type', function (cb) {
      var type = avsc.createType(path.join(DPATH, 'Id.avsc'));
      avsc.createFileDecoder(path.join(DPATH, 'person-10.avro'), type)
        .on('error', function  () { cb(); });
    });

    test('raw file', function (cb) {
      var type = avsc.createType(path.join(DPATH, 'Person.avsc'));
      var n = 0;
      avsc.createFileDecoder(path.join(DPATH, 'person-10.avro.raw'), type)
        .on('data', function (obj) {
          n++;
          assert(type.isValid(obj));
        })
        .on('end', function () {
          assert.equal(n, 10);
          cb();
        });
    });

  });

  test('extractFileHeader', function () {
    var header;
    var fpath = path.join(DPATH, 'person-10.avro');
    header = avsc.extractFileHeader(fpath);
    assert(header !== null);
    assert.equal(typeof header.meta['avro.schema'], 'object');
    header = avsc.extractFileHeader(fpath, {decode: false});
    assert(Buffer.isBuffer(header.meta['avro.schema']));
    header = avsc.extractFileHeader(fpath, {size: 2});
    assert.equal(typeof header.meta['avro.schema'], 'object');
    header = avsc.extractFileHeader(path.join(DPATH, 'person-10.avro.raw'));
    assert(header === null);
    header = avsc.extractFileHeader(
      path.join(DPATH, 'person-10.no-codec.avro')
    );
    assert(header !== null);
  });

});
