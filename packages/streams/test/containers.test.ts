'use strict';

const containers = require('../lib/containers'),
  types = require('../lib/types'),
  assert = require('assert'),
  buffer = require('buffer'),
  stream = require('stream'),
  zlib = require('zlib');

const Buffer = buffer.Buffer;
const BLOCK_TYPE = containers.BLOCK_TYPE;
const Block = BLOCK_TYPE.recordConstructor;
const HEADER_TYPE = containers.HEADER_TYPE;
const Header = HEADER_TYPE.recordConstructor;
const MAGIC_BYTES = containers.MAGIC_BYTES;
const SYNC = Buffer.from('atokensyncheader');
const Type = types.Type;
const streams = containers.streams;
const builtins = types.builtins;

const DECODER = new TextDecoder();

suite('containers', () => {
  suite('streams', () => {
    suite('RawEncoder', () => {
      const RawEncoder = streams.RawEncoder;

      test('flush once', (cb) => {
        const t = Type.forSchema('int');
        let buf;
        const encoder = new RawEncoder(t)
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
        const t = Type.forSchema('int');
        const bufs = [];
        const encoder = new RawEncoder(t, {batchSize: 1})
          .on('data', (chunk) => {
            bufs.push(chunk);
          })
          .on('end', () => {
            assert.deepEqual(bufs, [Buffer.from([1]), Buffer.from([2])]);
            cb();
          });
        encoder.write(-1);
        encoder.end(1);
      });

      test('resize', (cb) => {
        const t = Type.forSchema({type: 'fixed', name: 'A', size: 2});
        const data = Buffer.from([48, 18]);
        let buf;
        const encoder = new RawEncoder(t, {batchSize: 1})
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
        const t = Type.forSchema({type: 'fixed', name: 'A', size: 2});
        const data = Buffer.from([48, 18]);
        const chunks = [];
        const encoder = new RawEncoder(t, {batchSize: 2})
          .on('data', (chunk) => {
            chunks.push(chunk);
          })
          .on('end', () => {
            assert.deepEqual(chunks, [data, data]);
            cb();
          });
        encoder.write(data);
        encoder.write(data);
        encoder.end();
      });

      test('empty', (cb) => {
        const t = Type.forSchema('int');
        const chunks = [];
        const encoder = new RawEncoder(t, {batchSize: 2})
          .on('data', (chunk) => {
            chunks.push(chunk);
          })
          .on('end', () => {
            assert.deepEqual(chunks, []);
            cb();
          });
        encoder.end();
      });

      test('missing writer type', () => {
        assert.throws(() => {
          new RawEncoder();
        });
      });

      test('writer type from schema', () => {
        const encoder = new RawEncoder('int');
        assert(encoder._type instanceof builtins.IntType);
      });

      test('invalid object', (cb) => {
        const t = Type.forSchema('int');
        const encoder = new RawEncoder(t).on('error', () => {
          cb();
        });
        encoder.write('hi');
      });
    });

    suite('RawDecoder', () => {
      const RawDecoder = streams.RawDecoder;

      test('single item', (cb) => {
        const t = Type.forSchema('int');
        const objs = [];
        const decoder = new RawDecoder(t)
          .on('data', (obj) => {
            objs.push(obj);
          })
          .on('end', () => {
            assert.deepEqual(objs, [0]);
            cb();
          });
        decoder.end(Buffer.from([0]));
      });

      test('no writer type', () => {
        assert.throws(() => {
          new RawDecoder();
        });
      });

      test('decoding', (cb) => {
        const t = Type.forSchema('int');
        const objs = [];
        const decoder = new RawDecoder(t)
          .on('data', (obj) => {
            objs.push(obj);
          })
          .on('end', () => {
            assert.deepEqual(objs, [1, 2]);
            cb();
          });
        decoder.write(Buffer.from([2]));
        decoder.end(Buffer.from([4]));
      });

      test('no decoding', (cb) => {
        const t = Type.forSchema('int');
        const bufs = [Buffer.from([3]), Buffer.from([124])];
        const objs = [];
        const decoder = new RawDecoder(t, {noDecode: true})
          .on('data', (obj) => {
            objs.push(obj);
          })
          .on('end', () => {
            assert.deepEqual(objs, bufs);
            cb();
          });
        decoder.write(bufs[0]);
        decoder.end(bufs[1]);
      });

      test('write partial', (cb) => {
        const t = Type.forSchema('bytes');
        const objs = [];
        const decoder = new RawDecoder(t)
          .on('data', (obj) => {
            objs.push(obj);
          })
          .on('end', () => {
            assert.deepEqual(objs, [Buffer.from([6])]);
            cb();
          });
        decoder.write(Buffer.from([2]));
        // Let the first read go through (and return null).
        process.nextTick(() => {
          decoder.end(Buffer.from([6]));
        });
      });

      test('read before write', (cb) => {
        const t = Type.forSchema('int');
        const objs = [];
        const decoder = new RawDecoder(t)
          .on('data', (obj) => {
            objs.push(obj);
          })
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
      const BlockEncoder = streams.BlockEncoder;

      test('invalid type', () => {
        assert.throws(() => {
          new BlockEncoder();
        });
      });

      test('invalid codec', () => {
        const t = Type.forSchema('int');
        assert.throws(() => {
          new BlockEncoder(t, {codec: 'foo'});
        });
      });

      test('invalid metadata', () => {
        const t = Type.forSchema('int');
        assert.throws(() => {
          new BlockEncoder(t, {metadata: {bar: 'foo'}});
        }, /invalid metadata/);
      });

      test('invalid object', (cb) => {
        const t = Type.forSchema('int');
        const encoder = new BlockEncoder(t).on('error', () => {
          cb();
        });
        encoder.write('hi');
      });

      test('empty eager header', (cb) => {
        const t = Type.forSchema('int');
        const chunks = [];
        const encoder = new BlockEncoder(t, {writeHeader: true})
          .on('data', (chunk) => {
            chunks.push(chunk);
          })
          .on('end', () => {
            assert.equal(chunks.length, 1);
            cb();
          });
        encoder.end();
      });

      test('empty lazy header', (cb) => {
        const t = Type.forSchema('int');
        let pushed = false;
        const encoder = new BlockEncoder(t, {omitHeader: false})
          .on('data', () => {
            pushed = true;
          })
          .on('end', () => {
            assert(!pushed);
            cb();
          });
        encoder.end();
      });

      test('empty pipe', (cb) => {
        const t = Type.forSchema('int');
        const rs = new stream.Readable();
        rs._read = function () {
          this.push(null);
        };
        const ws = new stream.Writable().on('finish', () => {
          cb();
        });
        rs.pipe(new BlockEncoder(t)).pipe(ws);
      });

      test('flush on finish', (cb) => {
        const t = Type.forSchema('int');
        const chunks = [];
        const encoder = new BlockEncoder(t, {
          omitHeader: true,
          syncMarker: SYNC,
        })
          .on('data', (chunk) => {
            chunks.push(chunk);
          })
          .on('end', () => {
            assert.deepEqual(chunks, [
              Buffer.from([6]),
              Buffer.from([6]),
              Buffer.from([24, 0, 8]),
              SYNC,
            ]);
            cb();
          });
        encoder.write(12);
        encoder.write(0);
        encoder.end(4);
      });

      test('flush on finish slow codec', (cb) => {
        const t = Type.forSchema('int');
        let pushed = false;
        const encoder = new BlockEncoder(t, {
          blockSize: 1,
          codec: 'slow',
          codecs: {slow: slowCodec},
          writeHeader: false,
        })
          .on('data', () => {
            pushed = true;
          })
          .on('end', () => {
            assert(pushed);
            cb();
          });
        encoder.write(12);
        encoder.end();

        function slowCodec(buf, cb) {
          setTimeout(() => {
            cb(null, buf);
          }, 50);
        }
      });

      test('flush when full', (cb) => {
        const chunks = [];
        const encoder = new BlockEncoder(Type.forSchema('int'), {
          writeHeader: false,
          syncMarker: SYNC,
          blockSize: 2,
        })
          .on('data', (chunk) => {
            chunks.push(chunk);
          })
          .on('end', () => {
            assert.deepEqual(chunks, [
              Buffer.from([2]),
              Buffer.from([2]),
              Buffer.from([2]),
              SYNC,

              Buffer.from([2]),
              Buffer.from([4]),
              Buffer.from([128, 1]),
              SYNC,
            ]);
            cb();
          });
        encoder.write(1);
        encoder.end(64);
      });

      test('resize', (cb) => {
        const t = Type.forSchema({type: 'fixed', size: 8, name: 'Eight'});
        const buf = Buffer.from('abcdefgh');
        const chunks = [];
        const encoder = new BlockEncoder(t, {
          omitHeader: true,
          syncMarker: SYNC,
          blockSize: 4,
        })
          .on('data', (chunk) => {
            chunks.push(chunk);
          })
          .on('end', () => {
            const b1 = Buffer.from([4]);
            const b2 = Buffer.from([32]);
            assert.deepEqual(chunks, [b1, b2, Buffer.concat([buf, buf]), SYNC]);
            cb();
          });
        encoder.write(buf);
        encoder.end(buf);
      });

      test('compression error', (cb) => {
        const t = Type.forSchema('int');
        const codecs = {
          invalid (data, cb) {
            cb(new Error('ouch'));
          },
        };
        const encoder = new BlockEncoder(t, {codec: 'invalid', codecs}).on(
          'error',
          () => {
            cb();
          }
        );
        encoder.end(12);
      });

      test('write non-canonical schema', (cb) => {
        const obj = {type: 'fixed', size: 2, name: 'Id', doc: 'An id.'};
        const id = Buffer.from([1, 2]);
        const ids = [];
        const encoder = new BlockEncoder(obj);
        const decoder = new streams.BlockDecoder()
          .on('metadata', (type, codec, header) => {
            const schema = JSON.parse(DECODER.decode(header.meta['avro.schema']));
            assert.deepEqual(schema, obj); // Check that doc field not stripped.
          })
          .on('data', (id) => {
            ids.push(id);
          })
          .on('end', () => {
            assert.deepEqual(ids, [id]);
            cb();
          });
        encoder.pipe(decoder);
        encoder.end(id);
      });
    });

    suite('BlockDecoder', () => {
      const BlockDecoder = streams.BlockDecoder;

      test('invalid magic bytes', (cb) => {
        const decoder = new BlockDecoder()
          .on('data', () => {})
          .on('error', () => {
            cb();
          });
        decoder.write(Buffer.from([0, 3, 2]));
        decoder.write(Buffer.from([1]));
      });

      test('invalid sync marker', (cb) => {
        const decoder = new BlockDecoder()
          .on('data', () => {})
          .on('error', () => {
            cb();
          });
        const header = new Header(
          MAGIC_BYTES,
          {
            'avro.schema': Buffer.from('"int"'),
            'avro.codec': Buffer.from('null'),
          },
          SYNC
        );
        decoder.write(header.toBuffer());
        decoder.write(Buffer.from([0, 0])); // Empty block.
        decoder.end(Buffer.from('alongerstringthansixteenbytes'));
      });

      test('missing codec', (cb) => {
        const decoder = new BlockDecoder()
          .on('data', () => {})
          .on('end', () => {
            cb();
          });
        const header = new Header(
          MAGIC_BYTES,
          {'avro.schema': Buffer.from('"int"')},
          SYNC
        );
        decoder.end(header.toBuffer());
      });

      test('unknown codec', (cb) => {
        const decoder = new BlockDecoder()
          .on('data', () => {})
          .on('error', () => {
            cb();
          });
        const header = new Header(
          MAGIC_BYTES,
          {
            'avro.schema': Buffer.from('"int"'),
            'avro.codec': Buffer.from('"foo"'),
          },
          SYNC
        );
        decoder.end(header.toBuffer());
      });

      test('invalid schema', (cb) => {
        const decoder = new BlockDecoder()
          .on('data', () => {})
          .on('error', () => {
            cb();
          });
        const header = new Header(
          MAGIC_BYTES,
          {
            'avro.schema': Buffer.from('"int2"'),
            'avro.codec': Buffer.from('null'),
          },
          SYNC
        );
        decoder.end(header.toBuffer());
      });

      test('short header', (cb) => {
        const vals = [];
        const decoder = new BlockDecoder()
          .on('data', (val) => {
            vals.push(val);
          })
          .on('end', () => {
            assert.deepEqual(vals, [2]);
            cb();
          });
        const buf = new Header(
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
        const type = Type.forSchema('string');
        const decoder = new BlockDecoder()
          .on('data', () => {})
          .on('error', () => {
            cb();
          });
        const header = new Header(
          MAGIC_BYTES,
          {
            'avro.schema': Buffer.from('"string"'),
            'avro.codec': Buffer.from('null'),
          },
          SYNC
        );
        decoder.write(header.toBuffer());
        decoder.end(
          new Block(
            5,
            Buffer.concat([
              type.toBuffer('hi'),
              Buffer.from([77]), // Corrupt (negative length).
            ]),
            SYNC
          ).toBuffer()
        );
      });
    });
  });

  suite('encode & decode', () => {
    test('uncompressed int', (cb) => {
      const t = Type.forSchema('int');
      const objs = [];
      const encoderInfos = [];
      const decoderInfos = [];
      const encoder = new streams.BlockEncoder(t).on('block', (info) => {
        encoderInfos.push(info);
      });
      const decoder = new streams.BlockDecoder()
        .on('block', (info) => {
          decoderInfos.push(info);
        })
        .on('data', (obj) => {
          objs.push(obj);
        })
        .on('end', () => {
          assert.deepEqual(objs, [12, 23, 48]);
          const infos = [
            {valueCount: 3, rawDataLength: 3, compressedDataLength: 3},
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
      const t = Type.forSchema('int');
      const objs = [];
      const encoder = new streams.BlockEncoder(t);
      const decoder = new streams.BlockDecoder({noDecode: true})
        .on('data', (obj) => {
          objs.push(obj);
        })
        .on('end', () => {
          assert.deepEqual(objs, [Buffer.from([96])]);
          cb();
        });
      encoder.pipe(decoder);
      encoder.end(48);
    });

    test('uncompressed int after delay', (cb) => {
      const t = Type.forSchema('int');
      const objs = [];
      const encoder = new streams.BlockEncoder(t);
      const decoder = new streams.BlockDecoder();
      encoder.pipe(decoder);
      encoder.write(12);
      encoder.write(23);
      encoder.end(48);

      setTimeout(() => {
        decoder
          .on('data', (obj) => {
            objs.push(obj);
          })
          .on('end', () => {
            assert.deepEqual(objs, [12, 23, 48]);
            cb();
          });
      }, 100);
    });

    test('uncompressed empty record', (cb) => {
      const t = Type.forSchema({type: 'record', name: 'A', fields: []});
      const objs = [];
      const encoder = new streams.BlockEncoder(t);
      const decoder = new streams.BlockDecoder()
        .on('data', (obj) => {
          objs.push(obj);
        })
        .on('end', () => {
          assert.deepEqual(objs, [{}, {}]);
          cb();
        });
      encoder.pipe(decoder);
      encoder.write({});
      encoder.end({});
    });

    test('deflated records', (cb) => {
      const t = Type.forSchema({
        type: 'record',
        name: 'Person',
        fields: [
          {name: 'name', type: 'string'},
          {name: 'age', type: 'int'},
        ],
      });
      const Person = t.getRecordConstructor();
      const p1 = [new Person('Ann', 23), new Person('Bob', 25)];
      const p2 = [];
      const encoder = new streams.BlockEncoder(t, {
        codec: 'deflate',
        codecs: {deflate: zlib.deflateRaw},
      });
      const decoder = new streams.BlockDecoder({
        codecs: {deflate: zlib.inflateRaw},
      })
        .on('data', (obj) => {
          p2.push(obj);
        })
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
      const t = Type.forSchema('int');
      const codecs = {
        null (data, cb) {
          cb(new Error('ouch'));
        },
      };
      const encoder = new streams.BlockEncoder(t, {codec: 'null'});
      const decoder = new streams.BlockDecoder({codecs}).on('error', () => {
        cb();
      });
      encoder.pipe(decoder);
      encoder.end(1);
    });

    test('decompression late read', (cb) => {
      const chunks = [];
      const encoder = new streams.BlockEncoder(Type.forSchema('int'));
      const decoder = new streams.BlockDecoder();
      encoder.pipe(decoder);
      encoder.end(1);
      decoder
        .on('data', (chunk) => {
          chunks.push(chunk);
        })
        .on('end', () => {
          assert.deepEqual(chunks, [1]);
          cb();
        });
    });

    test('parse hook', (cb) => {
      const t1 = Type.forSchema({type: 'map', values: 'int'});
      const t2 = Type.forSchema({
        type: 'array',
        items: {
          name: 'Person',
          type: 'record',
          fields: [
            {name: 'name', type: 'string'},
            {name: 'age', type: 'int'},
          ],
        },
      });
      const Person = t2.getItemsType().getRecordConstructor();
      const persons = [];
      const encoder = new streams.BlockEncoder(t1);
      const decoder = new streams.BlockDecoder({parseHook})
        .on('data', (val) => {
          persons.push(val);
        })
        .on('end', () => {
          assert.deepEqual(persons, [
            [],
            [new Person('Ann', 23), new Person('Bob', 25)],
            [new Person('Celia', 48)],
          ]);
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
      const t1 = Type.forSchema({
        name: 'Person',
        type: 'record',
        fields: [{name: 'name', type: 'string'}],
      });
      const t2 = Type.forSchema({
        name: 'Person',
        type: 'record',
        fields: [
          {name: 'name', type: 'string'},
          {name: 'fullName', aliases: ['name'], type: ['null', 'string']},
          {name: 'age', type: ['null', 'int'], default: null},
        ],
      });
      const persons = [];
      const encoder = new streams.BlockEncoder(t1);
      const decoder = new streams.BlockDecoder({readerSchema: t2})
        .on('data', (val) => {
          persons.push(val);
        })
        .on('end', () => {
          assert.deepEqual(persons, [
            {name: 'Ann', fullName: 'Ann', age: null},
            {name: 'Jane', fullName: 'Jane', age: null},
          ]);
          cb();
        });
      encoder.pipe(decoder);
      encoder.write({name: 'Ann'});
      encoder.write({name: 'Jane'});
      encoder.end();
    });

    test('ignore serialization error', (cb) => {
      const data = [];
      let numErrs = 0;
      const encoder = new streams.BlockEncoder('int').on('error', () => {
        numErrs++;
      });
      const decoder = new streams.BlockDecoder()
        .on('data', (val) => {
          data.push(val);
        })
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
      const okVals = [];
      const badVals = [];
      const encoder = new streams.BlockEncoder('int')
        .removeAllListeners('typeError')
        .on('typeError', (err, val) => {
          badVals.push(val);
        });
      const decoder = new streams.BlockDecoder()
        .on('data', (val) => {
          okVals.push(val);
        })
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
      const t = Type.forSchema('string');
      const buf = t.toBuffer('hello');
      let sawBuf = false;
      const objs = [];
      const encoder = new streams.BlockEncoder(t, {metadata: {foo: buf}});
      const decoder = new streams.BlockDecoder()
        .on('metadata', (type, codec, header) => {
          assert.deepEqual(header.meta.foo, buf);
          sawBuf = true;
        })
        .on('data', (obj) => {
          objs.push(obj);
        })
        .on('end', () => {
          assert.deepEqual(objs, ['hi']);
          assert(sawBuf);
          cb();
        });
      encoder.pipe(decoder);
      encoder.end('hi');
    });

    test('empty block', (cb) => {
      const t = Type.forSchema('int');
      const vals = [];
      const decoder = new streams.BlockDecoder()
        .on('data', (val) => {
          vals.push(val);
        })
        .on('end', () => {
          assert.deepEqual(vals, [1, 2]);
          cb();
        });
      decoder.write(
        HEADER_TYPE.toBuffer({
          magic: MAGIC_BYTES,
          meta: {
            'avro.schema': Buffer.from('"int"'),
            'avro.codec': Buffer.from('null'),
          },
          sync: SYNC,
        })
      );
      decoder.write(
        BLOCK_TYPE.toBuffer({
          count: 1,
          data: t.toBuffer(1),
          sync: SYNC,
        })
      );
      decoder.write(
        BLOCK_TYPE.toBuffer({
          count: 0,
          data: Buffer.from([]),
          sync: SYNC,
        })
      );
      decoder.write(
        BLOCK_TYPE.toBuffer({
          count: 1,
          data: t.toBuffer(2),
          sync: SYNC,
        })
      );
      decoder.end();
    });
  });
});
