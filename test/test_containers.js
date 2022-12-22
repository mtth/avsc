'use strict';

let containers = require('../lib/containers'),
    types = require('../lib/types'),
    assert = require('assert'),
    buffer = require('buffer'),
    stream = require('stream'),
    zlib = require('zlib');


let Buffer = buffer.Buffer;
let BLOCK_TYPE = containers.BLOCK_TYPE;
let Block = BLOCK_TYPE.recordConstructor;
let HEADER_TYPE = containers.HEADER_TYPE;
let Header = HEADER_TYPE.recordConstructor;
let MAGIC_BYTES = containers.MAGIC_BYTES;
let SYNC = Buffer.from('atokensyncheader');
let Type = types.Type;
let streams = containers.streams;
let builtins = types.builtins;


suite('containers', () => {

  suite('streams', () => {

    suite('RawEncoder', () => {

      let RawEncoder = streams.RawEncoder;

      test('flush once', (cb) => {
        let t = Type.forSchema('int');
        let buf;
        let encoder = new RawEncoder(t)
          .on('data', (chunk) => {
            assert.strictEqual(buf, undefined);
            buf = chunk;
          })
          .on('end', () => {
            assert.deepEqual(buf, Buffer.from([2, 0, 3]));
            cb();
          });
        encoder.write(1);
        encoder.write(0);
        encoder.end(-2);
      });

      test('write multiple', (cb) => {
        let t = Type.forSchema('int');
        let bufs = [];
        let encoder = new RawEncoder(t, {batchSize: 1})
          .on('data', (chunk) => {
            bufs.push(chunk);
          })
          .on('end', () => {
            assert.deepEqual(bufs, [
              Buffer.from([1]),
              Buffer.from([2])
            ]);
            cb();
          });
        encoder.write(-1);
        encoder.end(1);
      });

      test('resize', (cb) => {
        let t = Type.forSchema({type: 'fixed', name: 'A', size: 2});
        let data = Buffer.from([48, 18]);
        let buf;
        let encoder = new RawEncoder(t, {batchSize: 1})
          .on('data', (chunk) => {
            assert.strictEqual(buf, undefined);
            buf = chunk;
          })
          .on('end', () => {
            assert.deepEqual(buf, data);
            cb();
          });
        encoder.write(data);
        encoder.end();
      });

      test('flush when full', (cb) => {
        let t = Type.forSchema({type: 'fixed', name: 'A', size: 2});
        let data = Buffer.from([48, 18]);
        let chunks = [];
        let encoder = new RawEncoder(t, {batchSize: 2})
          .on('data', (chunk) => { chunks.push(chunk); })
          .on('end', () => {
            assert.deepEqual(chunks, [data, data]);
            cb();
          });
        encoder.write(data);
        encoder.write(data);
        encoder.end();
      });

      test('empty', (cb) => {
        let t = Type.forSchema('int');
        let chunks = [];
        let encoder = new RawEncoder(t, {batchSize: 2})
          .on('data', (chunk) => { chunks.push(chunk); })
          .on('end', () => {
            assert.deepEqual(chunks, []);
            cb();
          });
        encoder.end();
      });

      test('missing writer type', () => {
        assert.throws(() => { new RawEncoder(); });
      });

      test('writer type from schema', () => {
        let encoder = new RawEncoder('int');
        assert(encoder._type instanceof builtins.IntType);
      });

      test('invalid object', (cb) => {
        let t = Type.forSchema('int');
        let encoder = new RawEncoder(t)
          .on('error', () => { cb(); });
        encoder.write('hi');
      });

    });

    suite('RawDecoder', () => {

      let RawDecoder = streams.RawDecoder;

      test('single item', (cb) => {
        let t = Type.forSchema('int');
        let objs = [];
        let decoder = new RawDecoder(t)
          .on('data', (obj) => { objs.push(obj); })
          .on('end', () => {
            assert.deepEqual(objs, [0]);
            cb();
          });
        decoder.end(Buffer.from([0]));
      });

      test('no writer type', () => {
        assert.throws(() => { new RawDecoder(); });
      });

      test('decoding', (cb) => {
        let t = Type.forSchema('int');
        let objs = [];
        let decoder = new RawDecoder(t)
          .on('data', (obj) => { objs.push(obj); })
          .on('end', () => {
            assert.deepEqual(objs, [1, 2]);
            cb();
          });
        decoder.write(Buffer.from([2]));
        decoder.end(Buffer.from([4]));
      });

      test('no decoding', (cb) => {
        let t = Type.forSchema('int');
        let bufs = [Buffer.from([3]), Buffer.from([124])];
        let objs = [];
        let decoder = new RawDecoder(t, {noDecode: true})
          .on('data', (obj) => { objs.push(obj); })
          .on('end', () => {
            assert.deepEqual(objs, bufs);
            cb();
          });
        decoder.write(bufs[0]);
        decoder.end(bufs[1]);
      });

      test('write partial', (cb) => {
        let t = Type.forSchema('bytes');
        let objs = [];
        let decoder = new RawDecoder(t)
          .on('data', (obj) => { objs.push(obj); })
          .on('end', () => {
            assert.deepEqual(objs, [Buffer.from([6])]);
            cb();
          });
        decoder.write(Buffer.from([2]));
        // Let the first read go through (and return null).
        process.nextTick(() => { decoder.end(Buffer.from([6])); });
      });

      test('read before write', (cb) => {
        let t = Type.forSchema('int');
        let objs = [];
        let decoder = new RawDecoder(t)
          .on('data', (obj) => { objs.push(obj); })
          .on('end', () => {
            assert.deepEqual(objs, [1]);
            cb();
          });
        setTimeout(() => {
          decoder.end(Buffer.from([2]));
        }, 50);
      });

    });

    suite('BlockEncoder', () => {

      let BlockEncoder = streams.BlockEncoder;

      test('invalid type', () => {
        assert.throws(() => { new BlockEncoder(); });
      });

      test('invalid codec', () => {
        let t = Type.forSchema('int');
        assert.throws(() => { new BlockEncoder(t, {codec: 'foo'}); });
      });

      test('invalid metadata', () => {
        let t = Type.forSchema('int');
        assert.throws(() => {
          new BlockEncoder(t, {metadata: {bar: 'foo'}});
        }, /invalid metadata/);
      });

      test('invalid object', (cb) => {
        let t = Type.forSchema('int');
        let encoder = new BlockEncoder(t)
          .on('error', () => { cb(); });
        encoder.write('hi');
      });

      test('empty eager header', (cb) => {
        let t = Type.forSchema('int');
        let chunks = [];
        let encoder = new BlockEncoder(t, {writeHeader: true})
          .on('data', (chunk) => { chunks.push(chunk); })
          .on('end', () => {
            assert.equal(chunks.length, 1);
            cb();
          });
        encoder.end();
      });

      test('empty lazy header', (cb) => {
        let t = Type.forSchema('int');
        let pushed = false;
        let encoder = new BlockEncoder(t, {omitHeader: false})
          .on('data', () => { pushed = true; })
          .on('end', () => {
            assert(!pushed);
            cb();
          });
        encoder.end();
      });

      test('empty pipe', (cb) => {
        let t = Type.forSchema('int');
        let rs = new stream.Readable();
        rs._read = function () { this.push(null); };
        let ws = new stream.Writable().on('finish', () => { cb(); });
        rs.pipe(new BlockEncoder(t)).pipe(ws);
      });

      test('flush on finish', (cb) => {
        let t = Type.forSchema('int');
        let chunks = [];
        let encoder = new BlockEncoder(t, {
          omitHeader: true,
          syncMarker: SYNC
        }).on('data', (chunk) => { chunks.push(chunk); })
          .on('end', () => {
            assert.deepEqual(chunks, [
              Buffer.from([6]),
              Buffer.from([6]),
              Buffer.from([24, 0, 8]),
              SYNC
            ]);
            cb();
          });
        encoder.write(12);
        encoder.write(0);
        encoder.end(4);
      });

      test('flush on finish slow codec', (cb) => {
        let t = Type.forSchema('int');
        let pushed = false;
        let encoder = new BlockEncoder(t, {
          blockSize: 1,
          codec: 'slow',
          codecs: {slow: slowCodec},
          writeHeader: false
        }).on('data', () => { pushed = true; })
          .on('end', () => {
            assert(pushed);
            cb();
          });
        encoder.write(12);
        encoder.end();

        function slowCodec(buf, cb) {
          setTimeout(() => { cb(null, buf); }, 50);
        }
      });

      test('flush when full', (cb) => {
        let chunks = [];
        let encoder = new BlockEncoder(Type.forSchema('int'), {
          writeHeader: false,
          syncMarker: SYNC,
          blockSize: 2
        }).on('data', (chunk) => { chunks.push(chunk); })
          .on('end', () => {
            assert.deepEqual(
              chunks,
              [
                Buffer.from([2]),
                Buffer.from([2]),
                Buffer.from([2]),
                SYNC,

                Buffer.from([2]),
                Buffer.from([4]),
                Buffer.from([128, 1]),
                SYNC
              ]
            );
            cb();
          });
        encoder.write(1);
        encoder.end(64);
      });

      test('resize', (cb) => {
        let t = Type.forSchema({type: 'fixed', size: 8, name: 'Eight'});
        let buf = Buffer.from('abcdefgh');
        let chunks = [];
        let encoder = new BlockEncoder(t, {
          omitHeader: true,
          syncMarker: SYNC,
          blockSize: 4
        }).on('data', (chunk) => { chunks.push(chunk); })
          .on('end', () => {
            let b1 = Buffer.from([4]);
            let b2 = Buffer.from([32]);
            assert.deepEqual(chunks, [b1, b2, Buffer.concat([buf, buf]), SYNC]);
            cb();
          });
        encoder.write(buf);
        encoder.end(buf);
      });

      test('compression error', (cb) => {
        let t = Type.forSchema('int');
        let codecs = {
          invalid: function (data, cb) { cb(new Error('ouch')); }
        };
        let encoder = new BlockEncoder(t, {codec: 'invalid', codecs})
          .on('error', () => { cb(); });
        encoder.end(12);
      });

      test('write non-canonical schema', (cb) => {
        let obj = {type: 'fixed', size: 2, name: 'Id', doc: 'An id.'};
        let id = Buffer.from([1, 2]);
        let ids = [];
        let encoder = new BlockEncoder(obj);
        let decoder = new streams.BlockDecoder()
          .on('metadata', (type, codec, header) => {
            let schema = JSON.parse(header.meta['avro.schema'].toString());
            assert.deepEqual(schema, obj); // Check that doc field not stripped.
          })
          .on('data', (id) => { ids.push(id); })
          .on('end', () => {
            assert.deepEqual(ids, [id]);
            cb();
          });
        encoder.pipe(decoder);
        encoder.end(id);
      });

    });

    suite('BlockDecoder', () => {

      let BlockDecoder = streams.BlockDecoder;

      test('invalid magic bytes', (cb) => {
        let decoder = new BlockDecoder()
          .on('data', () => {})
          .on('error', () => { cb(); });
        decoder.write(Buffer.from([0, 3, 2]));
        decoder.write(Buffer.from([1]));
      });

      test('invalid sync marker', (cb) => {
        let decoder = new BlockDecoder()
          .on('data', () => {})
          .on('error', () => { cb(); });
        let header = new Header(
          MAGIC_BYTES,
          {
            'avro.schema': Buffer.from('"int"'),
            'avro.codec': Buffer.from('null')
          },
          SYNC
        );
        decoder.write(header.toBuffer());
        decoder.write(Buffer.from([0, 0])); // Empty block.
        decoder.end(Buffer.from('alongerstringthansixteenbytes'));
      });

      test('missing codec', (cb) => {
        let decoder = new BlockDecoder()
          .on('data', () => {})
          .on('end', () => { cb(); });
        let header = new Header(
          MAGIC_BYTES,
          {'avro.schema': Buffer.from('"int"')},
          SYNC
        );
        decoder.end(header.toBuffer());
      });

      test('unknown codec', (cb) => {
        let decoder = new BlockDecoder()
          .on('data', () => {})
          .on('error', () => { cb(); });
        let header = new Header(
          MAGIC_BYTES,
          {
            'avro.schema': Buffer.from('"int"'),
            'avro.codec': Buffer.from('"foo"')
          },
          SYNC
        );
        decoder.end(header.toBuffer());
      });

      test('invalid schema', (cb) => {
        let decoder = new BlockDecoder()
          .on('data', () => {})
          .on('error', () => { cb(); });
        let header = new Header(
          MAGIC_BYTES,
          {
            'avro.schema': Buffer.from('"int2"'),
            'avro.codec': Buffer.from('null')
          },
          SYNC
        );
        decoder.end(header.toBuffer());
      });

      test('short header', (cb) => {
        let vals = [];
        let decoder = new BlockDecoder()
          .on('data', (val) => { vals.push(val); })
          .on('end', () => {
            assert.deepEqual(vals, [2]);
            cb();
          });
        let buf = new Header(
          MAGIC_BYTES,
          {'avro.schema': Buffer.from('"int"')},
          SYNC
        ).toBuffer();
        decoder.write(buf.subarray(0, 5)); // Part of header.
        decoder.write(buf.subarray(5));
        decoder.write(Buffer.from([2, 2, 4]));
        decoder.write(SYNC);
        decoder.end();
      });

      test('corrupt data', (cb) => {
        let type = Type.forSchema('string');
        let decoder = new BlockDecoder()
          .on('data', () => {})
          .on('error', () => { cb(); });
        let header = new Header(
          MAGIC_BYTES,
          {
            'avro.schema': Buffer.from('"string"'),
            'avro.codec': Buffer.from('null')
          },
          SYNC
        );
        decoder.write(header.toBuffer());
        decoder.end(new Block(
          5,
          Buffer.concat([
            type.toBuffer('hi'),
            Buffer.from([77]) // Corrupt (negative length).
          ]),
          SYNC
        ).toBuffer());
      });

    });

  });

  suite('encode & decode', () => {

    test('uncompressed int', (cb) => {
      let t = Type.forSchema('int');
      let objs = [];
      let encoderInfos = [];
      let decoderInfos = [];
      let encoder = new streams.BlockEncoder(t)
        .on('block', (info) => { encoderInfos.push(info); });
      let decoder = new streams.BlockDecoder()
        .on('block', (info) => { decoderInfos.push(info); })
        .on('data', (obj) => { objs.push(obj); })
        .on('end', () => {
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

    test('uncompressed int non decoded', (cb) => {
      let t = Type.forSchema('int');
      let objs = [];
      let encoder = new streams.BlockEncoder(t);
      let decoder = new streams.BlockDecoder({noDecode: true})
        .on('data', (obj) => { objs.push(obj); })
        .on('end', () => {
          assert.deepEqual(objs, [Buffer.from([96])]);
          cb();
        });
      encoder.pipe(decoder);
      encoder.end(48);
    });

    test('uncompressed int after delay', (cb) => {
      let t = Type.forSchema('int');
      let objs = [];
      let encoder = new streams.BlockEncoder(t);
      let decoder = new streams.BlockDecoder();
      encoder.pipe(decoder);
      encoder.write(12);
      encoder.write(23);
      encoder.end(48);

      setTimeout(() => {
        decoder
          .on('data', (obj) => { objs.push(obj); })
          .on('end', () => {
            assert.deepEqual(objs, [12, 23, 48]);
            cb();
          });
      }, 100);
    });

    test('uncompressed empty record', (cb) => {
      let t = Type.forSchema({type: 'record', name: 'A', fields: []});
      let objs = [];
      let encoder = new streams.BlockEncoder(t);
      let decoder = new streams.BlockDecoder()
        .on('data', (obj) => { objs.push(obj); })
        .on('end', () => {
          assert.deepEqual(objs, [{}, {}]);
          cb();
        });
      encoder.pipe(decoder);
      encoder.write({});
      encoder.end({});
    });

    test('deflated records', (cb) => {
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
      let encoder = new streams.BlockEncoder(t, {
        codec: 'deflate',
        codecs: {'deflate': zlib.deflateRaw},
      });
      let decoder = new streams.BlockDecoder({
        codecs: {'deflate': zlib.inflateRaw},
      }).on('data', (obj) => { p2.push(obj); })
        .on('end', () => {
          assert.deepEqual(p2, p1);
          cb();
        });
      encoder.pipe(decoder);
      for (let i = 0, l = p1.length; i < l; i++) {
        encoder.write(p1[i]);
      }
      encoder.end();
    });

    test('decompression error', (cb) => {
      let t = Type.forSchema('int');
      let codecs = {
        'null': function (data, cb) { cb(new Error('ouch')); }
      };
      let encoder = new streams.BlockEncoder(t, {codec: 'null'});
      let decoder = new streams.BlockDecoder({codecs})
        .on('error', () => { cb(); });
      encoder.pipe(decoder);
      encoder.end(1);
    });

    test('decompression late read', (cb) => {
      let chunks = [];
      let encoder = new streams.BlockEncoder(Type.forSchema('int'));
      let decoder = new streams.BlockDecoder();
      encoder.pipe(decoder);
      encoder.end(1);
      decoder.on('data', (chunk) => { chunks.push(chunk); })
        .on('end', () => {
          assert.deepEqual(chunks, [1]);
          cb();
        });
    });

    test('parse hook', (cb) => {
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
      let decoder = new streams.BlockDecoder({parseHook})
        .on('data', (val) => { persons.push(val); })
        .on('end', () => {
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

    test('reader type', (cb) => {
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
        .on('data', (val) => { persons.push(val); })
        .on('end', () => {
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

    test('ignore serialization error', (cb) => {
      let data = [];
      let numErrs = 0;
      let encoder = new streams.BlockEncoder('int')
        .on('error', () => { numErrs++; });
      let decoder = new streams.BlockDecoder()
        .on('data', (val) => { data.push(val); })
        .on('end', () => {
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

    test('custom type error handler', (cb) => {
      let okVals = [];
      let badVals = [];
      let encoder = new streams.BlockEncoder('int')
        .removeAllListeners('typeError')
        .on('typeError', (err, val) => { badVals.push(val); });
      let decoder = new streams.BlockDecoder()
        .on('data', (val) => { okVals.push(val); })
        .on('end', () => {
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

    test('metadata', (cb) => {
      let t = Type.forSchema('string');
      let buf = t.toBuffer('hello');
      let sawBuf = false;
      let objs = [];
      let encoder = new streams.BlockEncoder(t, {metadata: {foo: buf}});
      let decoder = new streams.BlockDecoder()
        .on('metadata', (type, codec, header) => {
          assert.deepEqual(header.meta.foo, buf);
          sawBuf = true;
        })
        .on('data', (obj) => { objs.push(obj); })
        .on('end', () => {
          assert.deepEqual(objs, ['hi']);
          assert(sawBuf);
          cb();
        });
      encoder.pipe(decoder);
      encoder.end('hi');
    });

    test('empty block', (cb) => {
      let t = Type.forSchema('int');
      let vals = [];
      let decoder = new streams.BlockDecoder()
        .on('data', (val) => { vals.push(val); })
        .on('end', () => {
          assert.deepEqual(vals, [1, 2]);
          cb();
        });
      decoder.write(HEADER_TYPE.toBuffer({
        magic: MAGIC_BYTES,
        meta: {
          'avro.schema': Buffer.from('"int"'),
          'avro.codec': Buffer.from('null')
        },
        sync: SYNC
      }));
      decoder.write(BLOCK_TYPE.toBuffer({
        count: 1, data: t.toBuffer(1), sync: SYNC
      }));
      decoder.write(BLOCK_TYPE.toBuffer({
        count: 0, data: Buffer.from([]), sync: SYNC
      }));
      decoder.write(BLOCK_TYPE.toBuffer({
        count: 1, data: t.toBuffer(2), sync: SYNC
      }));
      decoder.end();
    });

  });

});
