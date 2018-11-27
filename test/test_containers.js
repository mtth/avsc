/* jshint node: true, mocha: true */

'use strict';

var containers = require('../lib/containers'),
    types = require('../lib/types'),
    utils = require('../lib/utils'),
    assert = require('assert'),
    stream = require('stream');


var BLOCK_TYPE = containers.BLOCK_TYPE;
var HEADER_TYPE = containers.HEADER_TYPE;
var Header = HEADER_TYPE.getRecordConstructor();
var MAGIC_BYTES = containers.MAGIC_BYTES;
var SYNC = utils.bufferFrom('atokensyncheader');
var Type = types.Type;
var streams = containers.streams;
var builtins = types.builtins;


suite('containers', function () {

  suite('streams', function () {

    suite('RawEncoder', function () {

      var RawEncoder = streams.RawEncoder;

      test('flush once', function (cb) {
        var t = Type.forSchema('int');
        var buf;
        var encoder = new RawEncoder(t)
          .on('data', function (chunk) {
            assert.strictEqual(buf, undefined);
            buf = chunk;
          })
          .on('end', function () {
            assert.deepEqual(buf, utils.bufferFrom([2, 0, 3]));
            cb();
          });
        encoder.write(1);
        encoder.write(0);
        encoder.end(-2);
      });

      test('write multiple', function (cb) {
        var t = Type.forSchema('int');
        var bufs = [];
        var encoder = new RawEncoder(t, {batchSize: 1})
          .on('data', function (chunk) {
            bufs.push(chunk);
          })
          .on('end', function () {
            assert.deepEqual(bufs, [utils.bufferFrom([1]), utils.bufferFrom([2])]);
            cb();
          });
        encoder.write(-1);
        encoder.end(1);
      });

      test('resize', function (cb) {
        var t = Type.forSchema({type: 'fixed', name: 'A', size: 2});
        var data = utils.bufferFrom([48, 18]);
        var buf;
        var encoder = new RawEncoder(t, {batchSize: 1})
          .on('data', function (chunk) {
            assert.strictEqual(buf, undefined);
            buf = chunk;
          })
          .on('end', function () {
            assert.deepEqual(buf, data);
            cb();
          });
        encoder.write(data);
        encoder.end();
      });

      test('flush when full', function (cb) {
        var t = Type.forSchema({type: 'fixed', name: 'A', size: 2});
        var data = utils.bufferFrom([48, 18]);
        var chunks = [];
        var encoder = new RawEncoder(t, {batchSize: 2})
          .on('data', function (chunk) { chunks.push(chunk); })
          .on('end', function () {
            assert.deepEqual(chunks, [data, data]);
            cb();
          });
        encoder.write(data);
        encoder.write(data);
        encoder.end();
      });

      test('empty', function (cb) {
        var t = Type.forSchema('int');
        var chunks = [];
        var encoder = new RawEncoder(t, {batchSize: 2})
          .on('data', function (chunk) { chunks.push(chunk); })
          .on('end', function () {
            assert.deepEqual(chunks, []);
            cb();
          });
        encoder.end();
      });

      test('missing writer type', function () {
        assert.throws(function () { new RawEncoder(); });
      });

      test('writer type from schema', function () {
        var encoder = new RawEncoder('int');
        assert(encoder._type instanceof builtins.IntType);
      });

      test('invalid object', function (cb) {
        var t = Type.forSchema('int');
        var encoder = new RawEncoder(t)
          .on('error', function () { cb(); });
        encoder.write('hi');
      });

    });

    suite('RawDecoder', function () {

      var RawDecoder = streams.RawDecoder;

      test('single item', function (cb) {
        var t = Type.forSchema('int');
        var objs = [];
        var decoder = new RawDecoder(t)
          .on('data', function (obj) { objs.push(obj); })
          .on('end', function () {
            assert.deepEqual(objs, [0]);
            cb();
          });
        decoder.end(utils.bufferFrom([0]));
      });

      test('no writer type', function () {
        assert.throws(function () { new RawDecoder(); });
      });

      test('decoding', function (cb) {
        var t = Type.forSchema('int');
        var objs = [];
        var decoder = new RawDecoder(t)
          .on('data', function (obj) { objs.push(obj); })
          .on('end', function () {
            assert.deepEqual(objs, [1, 2]);
            cb();
          });
        decoder.write(utils.bufferFrom([2]));
        decoder.end(utils.bufferFrom([4]));
      });

      test('no decoding', function (cb) {
        var t = Type.forSchema('int');
        var bufs = [utils.bufferFrom([3]), utils.bufferFrom([124])];
        var objs = [];
        var decoder = new RawDecoder(t, {noDecode: true})
          .on('data', function (obj) { objs.push(obj); })
          .on('end', function () {
            assert.deepEqual(objs, bufs);
            cb();
          });
        decoder.write(bufs[0]);
        decoder.end(bufs[1]);
      });

      test('write partial', function (cb) {
        var t = Type.forSchema('bytes');
        var objs = [];
        var decoder = new RawDecoder(t)
          .on('data', function (obj) { objs.push(obj); })
          .on('end', function () {
            assert.deepEqual(objs, [utils.bufferFrom([6])]);
            cb();
          });
        decoder.write(utils.bufferFrom([2]));
        // Let the first read go through (and return null).
        process.nextTick(function () { decoder.end(utils.bufferFrom([6])); });
      });

      test('read before write', function (cb) {
        var t = Type.forSchema('int');
        var objs = [];
        var decoder = new RawDecoder(t)
          .on('data', function (obj) { objs.push(obj); })
          .on('end', function () {
            assert.deepEqual(objs, [1]);
            cb();
          });
        setTimeout(function () {
          decoder.end(utils.bufferFrom([2]));
        }, 50);
      });

    });

    suite('BlockEncoder', function () {

      var BlockEncoder = streams.BlockEncoder;

      test('invalid type', function () {
        assert.throws(function () { new BlockEncoder(); });
      });

      test('invalid codec', function () {
        var t = Type.forSchema('int');
        assert.throws(function () { new BlockEncoder(t, {codec: 'foo'}); });
      });

      test('invalid metadata', function () {
        var t = Type.forSchema('int');
        assert.throws(function () {
          new BlockEncoder(t, {metadata: {bar: 'foo'}});
        }, /invalid metadata/);
      });

      test('invalid object', function (cb) {
        var t = Type.forSchema('int');
        var encoder = new BlockEncoder(t)
          .on('error', function () { cb(); });
        encoder.write('hi');
      });

      test('empty eager header', function (cb) {
        var t = Type.forSchema('int');
        var chunks = [];
        var encoder = new BlockEncoder(t, {writeHeader: true})
          .on('data', function (chunk) { chunks.push(chunk); })
          .on('end', function () {
            assert.equal(chunks.length, 1);
            cb();
          });
        encoder.end();
      });

      test('empty lazy header', function (cb) {
        var t = Type.forSchema('int');
        var pushed = false;
        var encoder = new BlockEncoder(t, {omitHeader: false})
          .on('data', function () { pushed = true; })
          .on('end', function () {
            assert(!pushed);
            cb();
          });
        encoder.end();
      });

      test('empty pipe', function (cb) {
        var t = Type.forSchema('int');
        var rs = new stream.Readable();
        rs._read = function () { this.push(null); };
        var ws = new stream.Writable().on('finish', function () { cb(); });
        rs.pipe(new BlockEncoder(t)).pipe(ws);
      });

      test('flush on finish', function (cb) {
        var t = Type.forSchema('int');
        var chunks = [];
        var encoder = new BlockEncoder(t, {
          omitHeader: true,
          syncMarker: SYNC
        }).on('data', function (chunk) { chunks.push(chunk); })
          .on('end', function () {
            assert.deepEqual(chunks, [
              utils.bufferFrom([6]),
              utils.bufferFrom([6]),
              utils.bufferFrom([24, 0, 8]),
              SYNC
            ]);
            cb();
          });
        encoder.write(12);
        encoder.write(0);
        encoder.end(4);
      });

      test('flush on finish slow codec', function (cb) {
        var t = Type.forSchema('int');
        var pushed = false;
        var encoder = new BlockEncoder(t, {
          blockSize: 1,
          codec: 'slow',
          codecs: {slow: slowCodec},
          writeHeader: false
        }).on('data', function () { pushed = true; })
          .on('end', function () {
            assert(pushed);
            cb();
          });
        encoder.write(12);
        encoder.end();

        function slowCodec(buf, cb) {
          setTimeout(function () { cb(null, buf); }, 50);
        }
      });

      test('flush when full', function (cb) {
        var chunks = [];
        var encoder = new BlockEncoder(Type.forSchema('int'), {
          writeHeader: false,
          syncMarker: SYNC,
          blockSize: 2
        }).on('data', function (chunk) { chunks.push(chunk); })
          .on('end', function () {
            assert.deepEqual(
              chunks,
              [
                utils.bufferFrom([2]), utils.bufferFrom([2]), utils.bufferFrom([2]), SYNC,
                utils.bufferFrom([2]), utils.bufferFrom([4]), utils.bufferFrom([128, 1]), SYNC
              ]
            );
            cb();
          });
        encoder.write(1);
        encoder.end(64);
      });

      test('resize', function (cb) {
        var t = Type.forSchema({type: 'fixed', size: 8, name: 'Eight'});
        var buf = utils.bufferFrom('abcdefgh');
        var chunks = [];
        var encoder = new BlockEncoder(t, {
          omitHeader: true,
          syncMarker: SYNC,
          blockSize: 4
        }).on('data', function (chunk) { chunks.push(chunk); })
          .on('end', function () {
            var b1 = utils.bufferFrom([4]);
            var b2 = utils.bufferFrom([32]);
            assert.deepEqual(chunks, [b1, b2, Buffer.concat([buf, buf]), SYNC]);
            cb();
          });
        encoder.write(buf);
        encoder.end(buf);
      });

      test('compression error', function (cb) {
        var t = Type.forSchema('int');
        var codecs = {
          invalid: function (data, cb) { cb(new Error('ouch')); }
        };
        var encoder = new BlockEncoder(t, {codec: 'invalid', codecs: codecs})
          .on('error', function () { cb(); });
        encoder.end(12);
      });

      test('write non-canonical schema', function (cb) {
        var obj = {type: 'fixed', size: 2, name: 'Id', doc: 'An id.'};
        var id = utils.bufferFrom([1, 2]);
        var ids = [];
        var encoder = new BlockEncoder(obj);
        var decoder = new streams.BlockDecoder()
          .on('metadata', function (type, codec, header) {
            var schema = JSON.parse(header.meta['avro.schema'].toString());
            assert.deepEqual(schema, obj); // Check that doc field not stripped.
          })
          .on('data', function (id) { ids.push(id); })
          .on('end', function () {
            assert.deepEqual(ids, [id]);
            cb();
          });
        encoder.pipe(decoder);
        encoder.end(id);
      });

    });

    suite('BlockDecoder', function () {

      var BlockDecoder = streams.BlockDecoder;

      test('invalid magic bytes', function (cb) {
        var decoder = new BlockDecoder()
          .on('data', function () {})
          .on('error', function () { cb(); });
        decoder.write(utils.bufferFrom([0, 3, 2]));
        decoder.write(utils.bufferFrom([1]));
      });

      test('invalid sync marker', function (cb) {
        var decoder = new BlockDecoder()
          .on('data', function () {})
          .on('error', function () { cb(); });
        var header = new Header(
          MAGIC_BYTES,
          {
            'avro.schema': utils.bufferFrom('"int"'),
            'avro.codec': utils.bufferFrom('null')
          },
          SYNC
        );
        decoder.write(header.toBuffer());
        decoder.write(utils.bufferFrom([0, 0])); // Empty block.
        decoder.end(utils.bufferFrom('alongerstringthansixteenbytes'));
      });

      test('missing codec', function (cb) {
        var decoder = new BlockDecoder()
          .on('data', function () {})
          .on('end', function () { cb(); });
        var header = new Header(
          MAGIC_BYTES,
          {'avro.schema': utils.bufferFrom('"int"')},
          SYNC
        );
        decoder.end(header.toBuffer());
      });

      test('unknown codec', function (cb) {
        var decoder = new BlockDecoder()
          .on('data', function () {})
          .on('error', function () { cb(); });
        var header = new Header(
          MAGIC_BYTES,
          {
            'avro.schema': utils.bufferFrom('"int"'),
            'avro.codec': utils.bufferFrom('"foo"')
          },
          SYNC
        );
        decoder.end(header.toBuffer());
      });

      test('invalid schema', function (cb) {
        var decoder = new BlockDecoder()
          .on('data', function () {})
          .on('error', function () { cb(); });
        var header = new Header(
          MAGIC_BYTES,
          {
            'avro.schema': utils.bufferFrom('"int2"'),
            'avro.codec': utils.bufferFrom('null')
          },
          SYNC
        );
        decoder.end(header.toBuffer());
      });

      test('short header', function (cb) {
        var vals = [];
        var decoder = new BlockDecoder()
          .on('data', function (val) { vals.push(val); })
          .on('end', function () {
            assert.deepEqual(vals, [2]);
            cb();
          });
        var buf = new Header(
          MAGIC_BYTES,
          {'avro.schema': utils.bufferFrom('"int"')},
          SYNC
        ).toBuffer();
        decoder.write(buf.slice(0, 5)); // Part of header.
        decoder.write(buf.slice(5));
        decoder.write(utils.bufferFrom([2, 2, 4]));
        decoder.write(SYNC);
        decoder.end();
      });

    });

  });

  suite('encode & decode', function () {

    test('uncompressed int', function (cb) {
      var t = Type.forSchema('int');
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
      var t = Type.forSchema('int');
      var objs = [];
      var encoder = new streams.BlockEncoder(t);
      var decoder = new streams.BlockDecoder({noDecode: true})
        .on('data', function (obj) { objs.push(obj); })
        .on('end', function () {
          assert.deepEqual(objs, [utils.bufferFrom([96])]);
          cb();
        });
      encoder.pipe(decoder);
      encoder.end(48);
    });

    test('uncompressed int after delay', function (cb) {
      var t = Type.forSchema('int');
      var objs = [];
      var encoder = new streams.BlockEncoder(t);
      var decoder = new streams.BlockDecoder();
      encoder.pipe(decoder);
      encoder.write(12);
      encoder.write(23);
      encoder.end(48);

      setTimeout(function () {
        decoder
          .on('data', function (obj) { objs.push(obj); })
          .on('end', function () {
            assert.deepEqual(objs, [12, 23, 48]);
            cb();
          });
      }, 100);
    });

    test('uncompressed empty record', function (cb) {
      var t = Type.forSchema({type: 'record', name: 'A', fields: []});
      var objs = [];
      var encoder = new streams.BlockEncoder(t);
      var decoder = new streams.BlockDecoder()
        .on('data', function (obj) { objs.push(obj); })
        .on('end', function () {
          assert.deepEqual(objs, [{}, {}]);
          cb();
        });
      encoder.pipe(decoder);
      encoder.write({});
      encoder.end({});
    });

    test('deflated records', function (cb) {
      var t = Type.forSchema({
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

    test('decompression error', function (cb) {
      var t = Type.forSchema('int');
      var codecs = {
        'null': function (data, cb) { cb(new Error('ouch')); }
      };
      var encoder = new streams.BlockEncoder(t, {codec: 'null'});
      var decoder = new streams.BlockDecoder({codecs: codecs})
        .on('error', function () { cb(); });
      encoder.pipe(decoder);
      encoder.end(1);
    });

    test('decompression late read', function (cb) {
      var chunks = [];
      var encoder = new streams.BlockEncoder(Type.forSchema('int'));
      var decoder = new streams.BlockDecoder();
      encoder.pipe(decoder);
      encoder.end(1);
      decoder.on('data', function (chunk) { chunks.push(chunk); })
        .on('end', function () {
          assert.deepEqual(chunks, [1]);
          cb();
        });
    });

    test('parse hook', function (cb) {
      var t1 = Type.forSchema({type: 'map', values: 'int'});
      var t2 = Type.forSchema({
        type: 'array',
        items: {
          name: 'Person',
          type: 'record',
          fields: [
            {name: 'name', type: 'string'},
            {name: 'age', type: 'int'}
          ]
        }
      });
      var Person = t2.getItemsType().getRecordConstructor();
      var persons = [];
      var encoder = new streams.BlockEncoder(t1);
      var decoder = new streams.BlockDecoder({parseHook: parseHook})
        .on('data', function (val) { persons.push(val); })
        .on('end', function () {
          assert.deepEqual(
            persons,
            [
              [],
              [new Person('Ann', 23), new Person('Bob', 25)],
              [new Person('Celia', 48)]
            ]
          );
          cb();
        });
      encoder.pipe(decoder);
      encoder.write({});
      encoder.write({Ann: 23, Bob: 25});
      encoder.write({Celia: 48});
      encoder.end();

      function parseHook(schema) {
        assert.deepEqual(schema, t1.getSchema());
        return t2;
      }
    });

    test('reader type', function (cb) {
      var t1 = Type.forSchema({
        name: 'Person',
        type: 'record',
        fields: [
          {name: 'name', type: 'string'},
        ]
      });
      var t2 = Type.forSchema({
        name: 'Person',
        type: 'record',
        fields: [
          {name: 'name', type: 'string'},
          {name: 'fullName', aliases: ['name'], type: ['null', 'string']},
          {name: 'age', type: ['null', 'int'], 'default': null}
        ]
      });
      var persons = [];
      var encoder = new streams.BlockEncoder(t1);
      var decoder = new streams.BlockDecoder({readerSchema: t2})
        .on('data', function (val) { persons.push(val); })
        .on('end', function () {
          assert.deepEqual(
            persons,
            [
              {name: 'Ann', fullName: 'Ann', age: null},
              {name: 'Jane', fullName: 'Jane', age: null}
            ]
          );
          cb();
        });
      encoder.pipe(decoder);
      encoder.write({name: 'Ann'})
      encoder.write({name: 'Jane'})
      encoder.end();

      function parseHook(schema) {
        assert.deepEqual(schema, t1.getSchema());
        return t2;
      }
    });

    test('metadata', function (cb) {
      var t = Type.forSchema('string');
      var buf = t.toBuffer('hello');
      var sawBuf = false;
      var objs = [];
      var encoder = new streams.BlockEncoder(t, {metadata: {foo: buf}});
      var decoder = new streams.BlockDecoder()
        .on('metadata', function (type, codec, header) {
          assert.deepEqual(header.meta.foo, buf);
          sawBuf = true;
        })
        .on('data', function (obj) { objs.push(obj); })
        .on('end', function () {
          assert.deepEqual(objs, ['hi']);
          assert(sawBuf);
          cb();
        });
      encoder.pipe(decoder);
      encoder.end('hi');
    });

    test('empty block', function (cb) {
      var t = Type.forSchema('int');
      var vals = [];
      var decoder = new streams.BlockDecoder()
        .on('data', function (val) { vals.push(val); })
        .on('end', function () {
          assert.deepEqual(vals, [1, 2]);
          cb();
        });
      decoder.write(HEADER_TYPE.toBuffer({
        magic: MAGIC_BYTES,
        meta: {
          'avro.schema': utils.bufferFrom('"int"'),
          'avro.codec': utils.bufferFrom('null')
        },
        sync: SYNC
      }));
      decoder.write(BLOCK_TYPE.toBuffer({
        count: 1, data: t.toBuffer(1), sync: SYNC
      }));
      decoder.write(BLOCK_TYPE.toBuffer({
        count: 0, data: utils.bufferFrom([]), sync: SYNC
      }));
      decoder.write(BLOCK_TYPE.toBuffer({
        count: 1, data: t.toBuffer(2), sync: SYNC
      }));
      decoder.end();
    });

  });

});
