/* jshint node: true, mocha: true */

'use strict';

var index = require('../lib'),
    types = require('@avro/types'),
    assert = require('assert'),
    path = require('path'),
    tmp = require('tmp');

var DPATH = path.join(__dirname, 'data');

suite('index', function () {
  test('createFileDecoder', function (cb) {
    var n = 0;
    var type = types.Type.forSchema({
      name: 'Person',
      type: 'record',
      fields: [
        {name: 'name', type: 'string'},
        {name: 'age', type: ['null', 'int'], default: null},
        {
          name: 'gender',
          type: {name: 'Gender', type: 'enum', symbols: ['FEMALE', 'MALE']}
        },
        {
          name: 'address',
          type: {
            name: 'Address',
            type: 'record',
            fields: [{name: 'zipcode', type: 'int'}]
          }
        }
      ]
    });
    index.createFileDecoder(path.join(DPATH, 'person-10.avro'))
      .on('metadata', function (writerType) {
        assert.equal(writerType.toString(), type.toString());
      })
      .on('data', function (obj) {
        n++;
        assert(type.isValid(obj));
      })
      .on('end', function () {
        assert.equal(n, 10);
        cb();
      });
  });

  test('createFileEncoder', function (cb) {
    var type = types.Type.forSchema({
      type: 'record',
      name: 'Person',
      fields: [
        {name: 'name', type: 'string'},
        {name: 'age', type: 'int'}
      ]
    });
    var path = tmp.fileSync().name;
    var encoder = index.createFileEncoder(path, type);
    encoder.write({name: 'Ann', age: 32});
    encoder.end({name: 'Bob', age: 33});
    var n = 0;
    encoder.on('finish', function () {
      setTimeout(function () { // Hack to wait until the file is flushed.
        index.createFileDecoder(path)
          .on('data', function (obj) {
            n++;
            assert(type.isValid(obj));
          })
          .on('end', function () {
            assert.equal(n, 2);
            cb();
          });
      }, 50);
    });
  });

  test('extractFileHeader', function () {
    var header;
    var fpath = path.join(DPATH, 'person-10.avro');
    header = index.extractFileHeader(fpath);
    assert(header !== null);
    assert.equal(typeof header.meta['avro.schema'], 'object');
    header = index.extractFileHeader(fpath, {decode: false});
    assert(typeof header.meta['avro.schema'] == 'string');
    header = index.extractFileHeader(fpath, {size: 2});
    assert.equal(typeof header.meta['avro.schema'], 'object');
    header = index.extractFileHeader(path.join(DPATH, 'person-10.avro.raw'));
    assert(header === null);
    header = index.extractFileHeader(
      path.join(DPATH, 'person-10.no-codec.avro')
    );
    assert(header !== null);
  });
});
