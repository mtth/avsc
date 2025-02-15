'use strict';

if (process.browser) {
  return;
}

const index = require('../lib'),
  specs = require('../lib/specs'),
  types = require('../lib/types'),
  {isBufferLike} = require('../lib/utils'),
  assert = require('assert'),
  path = require('path'),
  tmp = require('tmp');

const DPATH = path.join(__dirname, 'dat');

suite('index', () => {
  test('createFileDecoder', (cb) => {
    let n = 0;
    const schema = specs.read(path.join(DPATH, 'Person.avsc'));
    const type = index.Type.forSchema(schema);
    index
      .createFileDecoder(path.join(DPATH, 'person-10.avro'))
      .on('metadata', (writerType) => {
        assert.equal(writerType.toString(), type.toString());
      })
      .on('data', (obj) => {
        n++;
        assert(type.isValid(obj));
      })
      .on('end', () => {
        assert.equal(n, 10);
        cb();
      });
  });

  test('createFileEncoder', (cb) => {
    const type = types.Type.forSchema({
      type: 'record',
      name: 'Person',
      fields: [
        {name: 'name', type: 'string'},
        {name: 'age', type: 'int'},
      ],
    });
    const path = tmp.fileSync().name;
    const encoder = index.createFileEncoder(path, type);
    encoder.write({name: 'Ann', age: 32});
    encoder.end({name: 'Bob', age: 33});
    let n = 0;
    encoder.on('finish', () => {
      setTimeout(() => {
        // Hack to wait until the file is flushed.
        index
          .createFileDecoder(path)
          .on('data', (obj) => {
            n++;
            assert(type.isValid(obj));
          })
          .on('end', () => {
            assert.equal(n, 2);
            cb();
          });
      }, 50);
    });
  });

  test('extractFileHeader', () => {
    let header;
    const fpath = path.join(DPATH, 'person-10.avro');
    header = index.extractFileHeader(fpath);
    assert(header !== null);
    assert.equal(typeof header.meta['avro.schema'], 'object');
    header = index.extractFileHeader(fpath, {decode: false});
    assert(isBufferLike(header.meta['avro.schema']));
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
