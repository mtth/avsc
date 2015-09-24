/* jshint node: true, mocha: true */

'use strict';

var streams = require('../lib/streams'),
    types = require('../lib/types'),
    assert = require('assert');


var fromSchema = types.Type.fromSchema;

suite('streams', function () {

  suite('RawEncoder', function () {

    var RawEncoder = streams.RawEncoder;

    test('flush once', function (cb) {
      var t = fromSchema('int');
      var buf;
      var encoder = new RawEncoder({writerType: t, batchSize: 100})
        .on('data', function (chunk) {
          assert.strictEqual(buf, undefined);
          buf = chunk;
        })
        .on('finish', function () {
          assert.deepEqual(buf, new Buffer([2, 0, 3]));
          cb();
        });
      encoder.write(1);
      encoder.write(0);
      encoder.end(-2);
    });

    test('resize', function (cb) {
      var t = fromSchema({type: 'fixed', name: 'A', size: 2});
      var data = new Buffer([48, 18]);
      var buf;
      var encoder = new RawEncoder({writerType: t, batchSize: 1})
        .on('data', function (chunk) {
          assert.strictEqual(buf, undefined);
          buf = chunk;
        })
        .on('finish', function () {
          assert.deepEqual(buf, data);
          cb();
        });
      encoder.write(data);
      encoder.end();
    });

    test('flush when full', function (cb) {
      var t = fromSchema({type: 'fixed', name: 'A', size: 2});
      var data = new Buffer([48, 18]);
      var chunks = [];
      var encoder = new RawEncoder({writerType: t, batchSize: 2})
        .on('data', function (chunk) { chunks.push(chunk); })
        .on('finish', function () {
          assert.deepEqual(chunks, [data, data]);
          cb();
        });
      encoder.write(data);
      encoder.write(data);
      encoder.end();
    });

    test('empty', function (cb) {
      var t = fromSchema('int');
      var chunks = [];
      var encoder = new RawEncoder({writerType: t, batchSize: 2})
        .on('data', function (chunk) { chunks.push(chunk); })
        .on('finish', function () {
          assert.deepEqual(chunks, []);
          cb();
        });
      encoder.end();
    });

    test('type inference', function (cb) {
      var Person = fromSchema({
        type: 'record',
        name: 'Person',
        fields: [{name: 'age', type: 'int'}]
      }).getRecordConstructor();
      var chunks = [];
      var encoder = new RawEncoder()
        .on('data', function (chunk) { chunks.push(chunk); })
        .on('finish', function () {
          assert.deepEqual(chunks, [new Buffer([50, 96])]);
          cb();
        });
      encoder.write(new Person(25));
      encoder.end(new Person(48));
    });

    test('missing writer type', function (cb) {
      var encoder = new RawEncoder()
        .on('error', function () { cb(); });
      encoder.write(1);
    });

  });

  suite('RawDecoder', function () {

    var RawDecoder = streams.RawDecoder;

    test('single item', function (cb) {
      var t = fromSchema('int');
      var objs = [];
      var decoder = new RawDecoder({writerType: t})
        .on('data', function (obj) { objs.push(obj); })
        .on('finish', function () {
          assert.deepEqual(objs, [0]);
          cb();
        });
      decoder.end(new Buffer([0]));
    });

    test('no writer type', function (cb) {
      var decoder = new RawDecoder()
        .on('error', function () { cb(); });
      decoder.write(new Buffer(1));
    });

    test('with reader type', function (cb) {
      var wt = fromSchema('int');
      var rt = fromSchema(['null', 'int']);
      var objs = [];
      var decoder = new RawDecoder({readerType: rt, writerType: wt})
        .on('data', function (obj) { objs.push(obj); })
        .on('finish', function () {
          assert.deepEqual(objs, [{'int': 2}]);
          cb();
        });
      decoder.end(new Buffer([4]));
    });

    test('invalid reader type', function (cb) {
      var wt = fromSchema('int');
      var rt = fromSchema('string');
      var decoder = new RawDecoder({readerType: rt, writerType: wt})
        .on('error', function () { cb(); });
      decoder.end(new Buffer([4]));
    });


  });

});
