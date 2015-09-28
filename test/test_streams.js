/* jshint node: true, mocha: true */

'use strict';

var streams = require('../lib/streams'),
    types = require('../lib/types'),
    utils = require('../lib/utils'),
    assert = require('assert'),
    path = require('path');


var fromSchema = types.Type.fromSchema;
var DPATH = path.join(__dirname, 'dat');
var SYNC = new Buffer('atokensyncheader');
var AvscError = utils.AvscError;
var Header = streams.BlockEncoder.HEADER_TYPE.getRecordConstructor();
var MAGIC_BYTES = streams.BlockEncoder.MAGIC_BYTES;


suite('streams', function () {

  suite('RawEncoder', function () {

    var RawEncoder = streams.RawEncoder;

    test('flush once', function (cb) {
      var t = fromSchema('int');
      var buf;
      var encoder = new RawEncoder(t)
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
      var encoder = new RawEncoder(t, {batchSize: 1})
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
      var encoder = new RawEncoder(t, {batchSize: 2})
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
      var encoder = new RawEncoder(t, {batchSize: 2})
        .on('data', function (chunk) { chunks.push(chunk); })
        .on('finish', function () {
          assert.deepEqual(chunks, []);
          cb();
        });
      encoder.end();
    });

    test('missing writer type', function () {
      assert.throws(function () { new RawEncoder(); }, AvscError);
    });

    test('missing writer type', function () {
      assert.throws(function () { new RawEncoder(); }, AvscError);
    });

    test('invalid object', function (cb) {
      var t = fromSchema('int');
      var encoder = new RawEncoder(t)
        .on('error', function () { cb(); });
      encoder.write('hi');
    });

  });

  suite('RawDecoder', function () {

    var RawDecoder = streams.RawDecoder;

    test('single item', function (cb) {
      var t = fromSchema('int');
      var objs = [];
      var decoder = new RawDecoder(t)
        .on('data', function (obj) { objs.push(obj); })
        .on('finish', function () {
          assert.deepEqual(objs, [0]);
          cb();
        });
      decoder.end(new Buffer([0]));
    });

    test('no writer type', function () {
      assert.throws(function () { new RawDecoder(); }, AvscError);
    });

    test('decoding', function (cb) {
      var t = fromSchema('int');
      var objs = [];
      var decoder = new RawDecoder(t)
        .on('data', function (obj) { objs.push(obj); })
        .on('finish', function () {
          assert.deepEqual(objs, [1, 2]);
          cb();
        });
      decoder.write(new Buffer([2]));
      decoder.end(new Buffer([4]));
    });

    test('no decoding', function (cb) {
      var t = fromSchema('int');
      var bufs = [new Buffer([3]), new Buffer([124])];
      var objs = [];
      var decoder = new RawDecoder(t, {decode: false})
        .on('data', function (obj) { objs.push(obj); })
        .on('finish', function () {
          assert.deepEqual(objs, bufs);
          cb();
        });
      decoder.write(bufs[0]);
      decoder.end(bufs[1]);
    });

    test('write partial', function (cb) {
      var t = fromSchema('bytes');
      var objs = [];
      var decoder = new RawDecoder(t)
        .on('data', function (obj) { objs.push(obj); })
        .on('finish', function () {
          assert.deepEqual(objs, [new Buffer([6])]);
          cb();
        });
      decoder.write(new Buffer([2]));
      // Let the first read go through (and return null).
      process.nextTick(function () { decoder.end(new Buffer([6])); });
    });


  });

  suite('BlockEncoder', function () {

    var BlockEncoder = streams.BlockEncoder;

    test('invalid type', function () {
      assert.throws(function () { new BlockEncoder(); }, AvscError);
    });

    test('invalid codec', function (cb) {
      var t = fromSchema('int');
      var encoder = new BlockEncoder(t, {codec: 'foo'})
        .on('error', function () { cb(); });
      encoder.write(2);
    });

    test('invalid object', function (cb) {
      var t = fromSchema('int');
      var encoder = new BlockEncoder(t)
        .on('error', function () { cb(); });
      encoder.write('hi');
    });

    test('empty', function (cb) {
      var t = fromSchema('int');
      var chunks = [];
      var encoder = new BlockEncoder(t)
        .on('data', function (chunk) { chunks.push(chunk); })
        .on('end', function () {
          assert.equal(chunks.length, 0);
          cb();
        });
      encoder.end();
    });

    test('flush', function (cb) {
      var t = fromSchema('int');
      var chunks = [];
      var encoder = new BlockEncoder(t, {
        omitHeader: true,
        syncMarker: SYNC
      }).on('data', function (chunk) { chunks.push(chunk); })
        .on('end', function () {
          assert.deepEqual(chunks, [
            new Buffer([6]),
            new Buffer([6]),
            new Buffer([24, 0, 8]),
            SYNC
          ]);
          cb();
        });
      encoder.write(12);
      encoder.write(0);
      encoder.end(4);
    });

    test('resize', function (cb) {
      var t = fromSchema({type: 'fixed', size: 8, name: 'Eight'});
      var buf = new Buffer('abcdefgh');
      var chunks = [];
      var encoder = new BlockEncoder(t, {
        omitHeader: true,
        syncMarker: SYNC,
        blockSize: 4
      }).on('data', function (chunk) { chunks.push(chunk); })
        .on('end', function () {
          var b1 = new Buffer([2]);
          var b2 = new Buffer([16]);
          assert.deepEqual(chunks, [b1, b2, buf, SYNC, b1, b2, buf, SYNC]);
          cb();
        });
      encoder.write(buf);
      encoder.end(buf);
    });

  });

  suite('BlockDecoder', function () {

    var BlockDecoder = streams.BlockDecoder;

    test('getHeader', function () {
      var header;
      var fpath = path.join(DPATH, 'person-10.avro');
      header = BlockDecoder.getHeader(fpath);
      assert(header !== null);
      assert.equal(typeof header.meta['avro.schema'], 'object');
      header = BlockDecoder.getHeader(fpath, {decode: false});
      assert(Buffer.isBuffer(header.meta['avro.schema']));
      header = BlockDecoder.getHeader(fpath, {size: 2});
      assert.equal(typeof header.meta['avro.schema'], 'object');
      header = BlockDecoder.getHeader(path.join(DPATH, 'person-10.avro.raw'));
      assert(header === null);
      header = BlockDecoder.getHeader(
        path.join(DPATH, 'person-10.no-codec.avro')
      );
      assert(header !== null);
    });

    test('invalid magic bytes', function (cb) {
      var decoder = new BlockDecoder()
        .on('data', function () {})
        .on('error', function () { cb(); });
      decoder.write(new Buffer([0, 3, 2, 1])); // !== MAGIC_BYTES
      decoder.write(new Buffer([0]));
      decoder.end(SYNC);
    });

    test('invalid sync marker', function (cb) {
      var decoder = new BlockDecoder()
        .on('data', function () {})
        .on('error', function () { cb(); });
      var header = new Header(
        MAGIC_BYTES,
        {
          'avro.schema': new Buffer('"int"'),
          'avro.codec': new Buffer('null')
        },
        SYNC
      );
      decoder.write(header.$encode());
      decoder.write(new Buffer([0, 0])); // Empty block.
      decoder.end(new Buffer('alongerstringthansixteenbytes'));
    });

    test('missing codec', function (cb) {
      var decoder = new BlockDecoder()
        .on('data', function () {})
        .on('end', function () { cb(); });
      var header = new Header(
        MAGIC_BYTES,
        {'avro.schema': new Buffer('"int"')},
        SYNC
      );
      decoder.end(header.$encode());
    });

    test('unknown codec', function (cb) {
      var decoder = new BlockDecoder()
        .on('data', function () {})
        .on('error', function () { cb(); });
      var header = new Header(
        MAGIC_BYTES,
        {
          'avro.schema': new Buffer('"int"'),
          'avro.codec': new Buffer('"foo"')
        },
        SYNC
      );
      decoder.end(header.$encode());
    });

    test('invalid schema', function (cb) {
      var decoder = new BlockDecoder()
        .on('data', function () {})
        .on('error', function () { cb(); });
      var header = new Header(
        MAGIC_BYTES,
        {
          'avro.schema': new Buffer('"int2"'),
          'avro.codec': new Buffer('null')
        },
        SYNC
      );
      decoder.end(header.$encode());
    });

  });

  suite('encode & decode', function () {

    test('uncompressed int', function (cb) {
      var t = fromSchema('int');
      var objs = [];
      var encoder = new streams.BlockEncoder(t);
      var decoder = new streams.BlockDecoder()
        .on('data', function (obj) { objs.push(obj); })
        .on('end', function () {
          assert.deepEqual(objs, [12, 23, 48]);
          cb();
        });
      encoder.pipe(decoder);
      encoder.write(12);
      encoder.write(23);
      encoder.end(48);
    });

    test('uncompressed int non decoded', function (cb) {
      var t = fromSchema('int');
      var objs = [];
      var encoder = new streams.BlockEncoder(t);
      var decoder = new streams.BlockDecoder({decode: false})
        .on('data', function (obj) { objs.push(obj); })
        .on('end', function () {
          assert.deepEqual(objs, [new Buffer([96])]);
          cb();
        });
      encoder.pipe(decoder);
      encoder.end(48);
    });

    test('deflated records', function (cb) {
      var t = fromSchema({
        type: 'record',
        name: 'Person',
        fields: [
          {name: 'name', type: 'string'},
          {name: 'age', type: 'int'}
        ]
      });
      var Person = t.getRecordConstructor();
      var p1 = [
        new Person('Ann', 23),
        new Person('Bob', 25)
      ];
      var p2 = [];
      var encoder = new streams.BlockEncoder(t, {codec: 'deflate'});
      var decoder = new streams.BlockDecoder()
        .on('data', function (obj) { p2.push(obj); })
        .on('end', function () {
          assert.deepEqual(p2, p1);
          cb();
        });
      encoder.pipe(decoder);
      var i, l;
      for (i = 0, l = p1.length; i < l; i++) {
        encoder.write(p1[i]);
      }
      encoder.end();
    });

  });

});
