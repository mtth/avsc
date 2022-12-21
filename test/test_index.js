'use strict';

if (process.browser) {
  return;
}

let index = require('../lib'),
    services = require('../lib/services'),
    types = require('../lib/types'),
    assert = require('assert'),
    buffer = require('buffer'),
    path = require('path'),
    tmp = require('tmp');

let Buffer = buffer.Buffer;

let DPATH = path.join(__dirname, 'dat');


suite('index', function () {

  suite('parse', function () {

    let parse = index.parse;

    test('type object', function () {
      let obj = {
        type: 'record',
        name: 'Person',
        fields: [{name: 'so', type: 'Person'}]
      };
      assert(parse(obj) instanceof types.builtins.RecordType);
    });

    test('protocol object', function () {
      let obj = {protocol: 'Foo'};
      assert(parse(obj) instanceof services.Service);
    });

    test('type instance', function () {
      let type = parse({
        type: 'record',
        name: 'Person',
        fields: [{name: 'so', type: 'Person'}]
      });
      assert.strictEqual(parse(type), type);
    });

    test('stringified type schema', function () {
      assert(parse('"int"') instanceof types.builtins.IntType);
    });

    test('type name', function () {
      assert(parse('double') instanceof types.builtins.DoubleType);
    });

    test('type schema file', function () {
      let t1 = parse({type: 'fixed', name: 'id.Id', size: 64});
      let t2 = parse(path.join(__dirname, 'dat', 'Id.avsc'));
      assert.deepEqual(JSON.stringify(t1), JSON.stringify(t2));
    });

  });

  test('createFileDecoder', function (cb) {
    let n = 0;
    let type = index.parse(path.join(DPATH, 'Person.avsc'));
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
    let type = types.Type.forSchema({
      type: 'record',
      name: 'Person',
      fields: [
        {name: 'name', type: 'string'},
        {name: 'age', type: 'int'}
      ]
    });
    let path = tmp.fileSync().name;
    let encoder = index.createFileEncoder(path, type);
    encoder.write({name: 'Ann', age: 32});
    encoder.end({name: 'Bob', age: 33});
    let n = 0;
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
    let header;
    let fpath = path.join(DPATH, 'person-10.avro');
    header = index.extractFileHeader(fpath);
    assert(header !== null);
    assert.equal(typeof header.meta['avro.schema'], 'object');
    header = index.extractFileHeader(fpath, {decode: false});
    assert(Buffer.isBuffer(header.meta['avro.schema']));
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
