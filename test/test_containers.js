'use strict';

let containers = require('../lib/containers'),
    types = require('../lib/types'),
    utils = require('../lib/utils'),
    assert = require('assert'),
    buffer = require('buffer'),
    stream = require('stream');


let Buffer = buffer.Buffer;
let BLOCK_TYPE = containers.BLOCK_TYPE;
let Block = BLOCK_TYPE.recordConstructor;
let HEADER_TYPE = containers.HEADER_TYPE;
let Header = HEADER_TYPE.recordConstructor;
let MAGIC_BYTES = containers.MAGIC_BYTES;
let SYNC = utils.bufferFrom('atokensyncheader');
let Type = types.Type;
let streams = containers.streams;
let builtins = types.builtins;


suite('containers', function () {

  suite('streams', function () {

    suite('RawEncoder', function () {

      let RawEncoder = streams.RawEncoder;

      test('flush once', function (cb) {
        let t = Type.forSchema('int');
        let buf;
        let encoder = new RawEncoder(t)
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
        let t = Type.forSchema('int');
        let bufs = [];
        let encoder = new RawEncoder(t, {batchSize: 1})
          .on('data', function (chunk) {
            bufs.push(chunk);
          })
          .on('end', function () {
            assert.deepEqual(bufs, [
              utils.bufferFrom([1]),
              utils.bufferFrom([2])
            ]);
            cb();
          });
        encoder.write(-1);
        encoder.end(1);
      });

      test('resize', function (cb) {
        let t = Type.forSchema({type: 'fixed', name: 'A', size: 2});
        let data = utils.bufferFrom([48, 18]);
        let buf;
        let encoder = new RawEncoder(t, {batchSize: 1})
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
        let t = Type.forSchema({type: 'fixed', name: 'A', size: 2});
        let data = utils.bufferFrom([48, 18]);
        let chunks = [];
        let encoder = new RawEncoder(t, {batchSize: 2})
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
        let t = Type.forSchema('int');
        let chunks = [];
        let encoder = new RawEncoder(t, {batchSize: 2})
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
        let encoder = new RawEncoder('int');
        assert(encoder._type instanceof builtins.IntType);
      });

      test('invalid object', function (cb) {
        let t = Type.forSchema('int');
        let encoder = new RawEncoder(t)
          .on('error', function () { cb(); });
        encoder.write('hi');
      });

    });

    suite('RawDecoder', function () {

      let RawDecoder = streams.RawDecoder;

      test('single item', function (cb) {
        let t = Type.forSchema('int');
        let objs = [];
        let decoder = new RawDecoder(t)
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
        let t = Type.forSchema('int');
        let objs = [];
        let decoder = new RawDecoder(t)
          .on('data', function (obj) { objs.push(obj); })
          .on('end', function () {
            assert.deepEqual(objs, [1, 2]);
            cb();
          });
        decoder.write(utils.bufferFrom([2]));
        decoder.end(utils.bufferFrom([4]));
      });

      test('no decoding', function (cb) {
        let t = Type.forSchema('int');
        let bufs = [utils.bufferFrom([3]), utils.bufferFrom([124])];
        let objs = [];
        let decoder = new RawDecoder(t, {noDecode: true})
          .on('data', function (obj) { objs.push(obj); })
          .on('end', function () {
            assert.deepEqual(objs, bufs);
            cb();
          });
        decoder.write(bufs[0]);
        decoder.end(bufs[1]);
      });

      test('write partial', function (cb) {
        let t = Type.forSchema('bytes');
        let objs = [];
        let decoder = new RawDecoder(t)
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
        let t = Type.forSchema('int');
        let objs = [];
        let decoder = new RawDecoder(t)
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

      let BlockEncoder = streams.BlockEncoder;

      test('invalid type', function () {
        assert.throws(function () { new BlockEncoder(); });
      });

      test('invalid codec', function () {
        let t = Type.forSchema('int');
        assert.throws(function () { new BlockEncoder(t, {codec: 'foo'}); });
      });

      test('invalid metadata', function () {
        let t = Type.forSchema('int');
        assert.throws(function () {
          new BlockEncoder(t, {metadata: {bar: 'foo'}});
        }, /invalid metadata/);
      });

      test('invalid object', function (cb) {
        let t = Type.forSchema('int');
        let encoder = new BlockEncoder(t)
          .on('error', function () { cb(); });
        encoder.write('hi');
      });

      test('empty eager header', function (cb) {
        let t = Type.forSchema('int');
        let chunks = [];
        let encoder = new BlockEncoder(t, {writeHeader: true})
          .on('data', function (chunk) { chunks.push(chunk); })
          .on('end', function () {
            assert.equal(chunks.length, 1);
            cb();
          });
        encoder.end();
      });

      test('empty lazy header', function (cb) {
        let t = Type.forSchema('int');
        let pushed = false;
        let encoder = new BlockEncoder(t, {omitHeader: false})
          .on('data', function () { pushed = true; })
          .on('end', function () {
            assert(!pushed);
            cb();
          });
        encoder.end();
      });

      test('empty pipe', function (cb) {
        let t = Type.forSchema('int');
        let rs = new stream.Readable();
        rs._read = function () { this.push(null); };
        let ws = new stream.Writable().on('finish', function () { cb(); });
        rs.pipe(new BlockEncoder(t)).pipe(ws);
      });

      test('flush on finish', function (cb) {
        let t = Type.forSchema('int');
        let chunks = [];
        let encoder = new BlockEncoder(t, {
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
        let t = Type.forSchema('int');
        let pushed = false;
        let encoder = new BlockEncoder(t, {
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
        let chunks = [];
        let encoder = new BlockEncoder(Type.forSchema('int'), {
          writeHeader: false,
          syncMarker: SYNC,
          blockSize: 2
        }).on('data', function (chunk) { chunks.push(chunk); })
          .on('end', function () {
            assert.deepEqual(
              chunks,
              [
                utils.bufferFrom([2]),
                utils.bufferFrom([2]),
                utils.bufferFrom([2]),
                SYNC,

                utils.bufferFrom([2]),
                utils.bufferFrom([4]),
                utils.bufferFrom([128, 1]),
                SYNC
              ]
            );
            cb();
          });
        encoder.write(1);
        encoder.end(64);
      });

      test('resize', function (cb) {
        let t = Type.forSchema({type: 'fixed', size: 8, name: 'Eight'});
        let buf = utils.bufferFrom('abcdefgh');
        let chunks = [];
        let encoder = new BlockEncoder(t, {
          omitHeader: true,
          syncMarker: SYNC,
          blockSize: 4
        }).on('data', function (chunk) { chunks.push(chunk); })
          .on('end', function () {
            let b1 = utils.bufferFrom([4]);
            let b2 = utils.bufferFrom([32]);
            assert.deepEqual(chunks, [b1, b2, Buffer.concat([buf, buf]), SYNC]);
            cb();
          });
        encoder.write(buf);
        encoder.end(buf);
      });

      test('compression error', function (cb) {
        let t = Type.forSchema('int');
        let codecs = {
          invalid: function (data, cb) { cb(new Error('ouch')); }
        };
        let encoder = new BlockEncoder(t, {codec: 'invalid', codecs: codecs})
          .on('error', function () { cb(); });
        encoder.end(12);
      });

      test('write non-canonical schema', function (cb) {
        let obj = {type: 'fixed', size: 2, name: 'Id', doc: 'An id.'};
        let id = utils.bufferFrom([1, 2]);
        let ids = [];
        let encoder = new BlockEncoder(obj);
        let decoder = new streams.BlockDecoder()
          .on('metadata', function (type, codec, header) {
            let schema = JSON.parse(header.meta['avro.schema'].toString());
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

      let BlockDecoder = streams.BlockDecoder;

      test('invalid magic bytes', function (cb) {
        let decoder = new BlockDecoder()
          .on('data', function () {})
          .on('error', function () { cb(); });
        decoder.write(utils.bufferFrom([0, 3, 2]));
        decoder.write(utils.bufferFrom([1]));
      });

      test('invalid sync marker', function (cb) {
        let decoder = new BlockDecoder()
          .on('data', function () {})
          .on('error', function () { cb(); });
        let header = new Header(
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
        let decoder = new BlockDecoder()
          .on('data', function () {})
          .on('end', function () { cb(); });
        let header = new Header(
          MAGIC_BYTES,
          {'avro.schema': utils.bufferFrom('"int"')},
          SYNC
        );
        decoder.end(header.toBuffer());
      });

      test('unknown codec', function (cb) {
        let decoder = new BlockDecoder()
          .on('data', function () {})
          .on('error', function () { cb(); });
        let header = new Header(
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
        let decoder = new BlockDecoder()
          .on('data', function () {})
          .on('error', function () { cb(); });
        let header = new Header(
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
        let vals = [];
        let decoder = new BlockDecoder()
          .on('data', function (val) { vals.push(val); })
          .on('end', function () {
            assert.deepEqual(vals, [2]);
            cb();
          });
        let buf = new Header(
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

      test('corrupt data', function (cb) {
        let type = Type.forSchema('string');
        let decoder = new BlockDecoder()
          .on('data', function () {})
          .on('error', function () { cb(); });
        let header = new Header(
          MAGIC_BYTES,
          {
            'avro.schema': utils.bufferFrom('"string"'),
            'avro.codec': utils.bufferFrom('null')
          },
          SYNC
        );
        decoder.write(header.toBuffer());
        decoder.end(new Block(
          5,
          Buffer.concat([
            type.toBuffer('hi'),
            utils.bufferFrom([77]) // Corrupt (negative length).
          ]),
          SYNC
        ).toBuffer());
      });

    });

  });

  suite('encode & decode', function () {

    test('uncompressed int', function (cb) {
      let t = Type.forSchema('int');
      let objs = [];
      let encoderInfos = [];
      let decoderInfos = [];
      let encoder = new streams.BlockEncoder(t)
        .on('block', function (info) { encoderInfos.push(info); });
      let decoder = new streams.BlockDecoder()
        .on('block', function (info) { decoderInfos.push(info); })
        .on('data', function (obj) { objs.push(obj); })
        .on('end', function () {
          assert.deepEqual(objs, [12, 23, 48]);
          let infos = [
            {valueCount: 3, rawDataLength: 3, compressedDataLength: 3}
          ];
          assert.deepEqual(encoderInfos, infos);
          assert.deepEqual(decoderInfos, infos);
          cb();
        });
      encoder.pipe(decoder);
      encoder.write(12);
      encoder.write(23);
      encoder.end(48);
    });

    test('uncompressed int non decoded', function (cb) {
      let t = Type.forSchema('int');
      let objs = [];
      let encoder = new streams.BlockEncoder(t);
      let decoder = new streams.BlockDecoder({noDecode: true})
        .on('data', function (obj) { objs.push(obj); })
        .on('end', function () {
          assert.deepEqual(objs, [utils.bufferFrom([96])]);
          cb();
        });
      encoder.pipe(decoder);
      encoder.end(48);
    });

    test('uncompressed int after delay', function (cb) {
      let t = Type.forSchema('int');
      let objs = [];
      let encoder = new streams.BlockEncoder(t);
      let decoder = new streams.BlockDecoder();
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
      let t = Type.forSchema({type: 'record', name: 'A', fields: []});
      let objs = [];
      let encoder = new streams.BlockEncoder(t);
      let decoder = new streams.BlockDecoder()
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
      let t = Type.forSchema({
        type: 'record',
        name: 'Person',
        fields: [
          {name: 'name', type: 'string'},
          {name: 'age', type: 'int'}
        ]
      });
      let Person = t.getRecordConstructor();
      let p1 = [
        new Person('Ann', 23),
        new Person('Bob', 25)
      ];
      let p2 = [];
      let encoder = new streams.BlockEncoder(t, {codec: 'deflate'});
      let decoder = new streams.BlockDecoder()
        .on('data', function (obj) { p2.push(obj); })
        .on('end', function () {
          assert.deepEqual(p2, p1);
          cb();
        });
      encoder.pipe(decoder);
      let i, l;
      for (i = 0, l = p1.length; i < l; i++) {
        encoder.write(p1[i]);
      }
      encoder.end();
    });

    test('decompression error', function (cb) {
      let t = Type.forSchema('int');
      let codecs = {
        'null': function (data, cb) { cb(new Error('ouch')); }
      };
      let encoder = new streams.BlockEncoder(t, {codec: 'null'});
      let decoder = new streams.BlockDecoder({codecs: codecs})
        .on('error', function () { cb(); });
      encoder.pipe(decoder);
      encoder.end(1);
    });

    test('decompression late read', function (cb) {
      let chunks = [];
      let encoder = new streams.BlockEncoder(Type.forSchema('int'));
      let decoder = new streams.BlockDecoder();
      encoder.pipe(decoder);
      encoder.end(1);
      decoder.on('data', function (chunk) { chunks.push(chunk); })
        .on('end', function () {
          assert.deepEqual(chunks, [1]);
          cb();
        });
    });

    test('parse hook', function (cb) {
      let t1 = Type.forSchema({type: 'map', values: 'int'});
      let t2 = Type.forSchema({
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
      let Person = t2.getItemsType().getRecordConstructor();
      let persons = [];
      let encoder = new streams.BlockEncoder(t1);
      let decoder = new streams.BlockDecoder({parseHook: parseHook})
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
      let t1 = Type.forSchema({
        name: 'Person',
        type: 'record',
        fields: [
          {name: 'name', type: 'string'},
        ]
      });
      let t2 = Type.forSchema({
        name: 'Person',
        type: 'record',
        fields: [
          {name: 'name', type: 'string'},
          {name: 'fullName', aliases: ['name'], type: ['null', 'string']},
          {name: 'age', type: ['null', 'int'], 'default': null}
        ]
      });
      let persons = [];
      let encoder = new streams.BlockEncoder(t1);
      let decoder = new streams.BlockDecoder({readerSchema: t2})
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
      encoder.write({name: 'Ann'});
      encoder.write({name: 'Jane'});
      encoder.end();
    });

    test('ignore serialization error', function (cb) {
      let data = [];
      let numErrs = 0;
      let encoder = new streams.BlockEncoder('int')
        .on('error', function () { numErrs++; });
      let decoder = new streams.BlockDecoder()
        .on('data', function (val) { data.push(val); })
        .on('end', function () {
          assert.equal(numErrs, 2);
          assert.deepEqual(data, [1, 2, 3]);
          cb();
        });
      encoder.pipe(decoder);
      encoder.write(1);
      encoder.write('foo');
      encoder.write(2);
      encoder.write(3);
      encoder.write(4.5);
      encoder.end();
    });

    test('custom type error handler', function (cb) {
      let okVals = [];
      let badVals = [];
      let encoder = new streams.BlockEncoder('int')
        .removeAllListeners('typeError')
        .on('typeError', function (err, val) { badVals.push(val); });
      let decoder = new streams.BlockDecoder()
        .on('data', function (val) { okVals.push(val); })
        .on('end', function () {
          assert.deepEqual(okVals, [1, 2]);
          assert.deepEqual(badVals, ['foo', 5.4]);
          cb();
        });
      encoder.pipe(decoder);
      encoder.write('foo');
      encoder.write(1);
      encoder.write(2);
      encoder.write(5.4);
      encoder.end();
    });

    test('metadata', function (cb) {
      let t = Type.forSchema('string');
      let buf = t.toBuffer('hello');
      let sawBuf = false;
      let objs = [];
      let encoder = new streams.BlockEncoder(t, {metadata: {foo: buf}});
      let decoder = new streams.BlockDecoder()
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
      let t = Type.forSchema('int');
      let vals = [];
      let decoder = new streams.BlockDecoder()
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
