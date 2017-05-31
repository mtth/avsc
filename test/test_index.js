/* jshint node: true, mocha: true */

'use strict';

if (process.browser) {
  return;
}

var index = require('../lib'),
    services = require('../lib/services'),
    types = require('../lib/types'),
    assert = require('assert'),
    rmdir = require('rmdir'),
    path = require('path'),
    tmp = require('tmp');


var DPATH = path.join(__dirname, 'dat');


suite('index', function () {

  suite('parse', function () {

    var parse = index.parse;

    test('type object', function () {
      var obj = {
        type: 'record',
        name: 'Person',
        fields: [{name: 'so', type: 'Person'}]
      };
      assert(parse(obj) instanceof types.builtins.RecordType);
    });

    test('protocol object', function () {
      var obj = {protocol: 'Foo'};
      assert(parse(obj) instanceof services.Service);
    });

    test('type instance', function () {
      var type = parse({
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
      var t1 = parse({type: 'fixed', name: 'id.Id', size: 64});
      var t2 = parse(path.join(__dirname, 'dat', 'Id.avsc'));
      assert.deepEqual(JSON.stringify(t1), JSON.stringify(t2));
    });

  });

  test('createFileDecoder', function (cb) {
    var n = 0;
    var type = index.parse(path.join(DPATH, 'Person.avsc'));
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
      index.createFileDecoder(path)
        .on('data', function (obj) {
          n++;
          assert(type.isValid(obj));
        })
        .on('end', function () {
          assert.equal(n, 2);
          cb();
        });
    });
  });

  test('FileAppenderScalability', function (cb) {
    var batches = 3;
    var batchWrites = 40;
    var tmpPath = tmp.fileSync().name;
    var path = './temp/FileAppenderScalability/test/subdir' + tmpPath;
    var type = types.Type.forSchema({
      type: 'record',
      name: 'Person',
      fields: [
        {name: 'id', type: 'string'},
        {name: 'name', type: 'string'},
        {name: 'longString', type: 'string'}
      ]
    });
    var longString = (new Array(100*1024)).join("x");
    var record = {id: '1', name: 'Ann', longString: longString};
    
    var encoderBuilderFunction = function (path, type, opts) {
       return new index.createFileAppender(path, type, opts);
    };

    this.timeout(20000);
    writeToEncoder(encoderBuilderFunction, path, type, record, batchWrites, batches, function() {
      rmdir('./temp', function (err, dirs, files) {
        cb();
      });
    });
  });

  test('FileAppenderFlushing', function (cb) {
    var path = tmp.fileSync().name;
    var type = types.Type.forSchema({
      type: 'record',
      name: 'Person',
      fields: [
        {name: 'id', type: 'string'},
        {name: 'name', type: 'string'},
        {name: 'longString', type: 'string'}
      ]
    });
    var longString = (new Array(100*1024)).join("x");
    var record = {id: '1', name: 'Ann', longString: longString };
    
    function encoderBuilderFunction(path, type, opts) {
       opts = opts || {};
       opts.writeFlushed = true;
       return new index.createFileAppender(path, type, opts);
    };

    this.timeout(20000);
    testFlushing(encoderBuilderFunction, path, type, record, cb);
  });

  test('extractFileHeader', function () {
    var header;
    var fpath = path.join(DPATH, 'person-10.avro');
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

function testFlushing(encoderBuilderFunction, path, type, record, cb) {
    var batchRun = 1;
    var batchWrites = 40;
    var encoder = encoderBuilderFunction(path, type);
    var recordStore = {};
    var flushTimeSLA = 500;

    function writeCallback(record) {
      recordStore[record.id] = record;

      // Asserts the record is both flushed and read back inside a given SLA
      setTimeout(function () {
        assert(!recordStore[record.id]);
      }, flushTimeSLA);
    }

    scalableWrite(encoder, record, writeCallback, null, null, batchRun, batchWrites);
    startAssertingScalableWrites(path, type, recordStore, batchRun, batchWrites, null , cb)
  }


function writeToEncoder(encoderBuilderFunction, path, type, record, batchWrites, batches, cb) {
    var batchRun = 1;
    var encoder = null;
    var recordStore = {};

    function writeCallback(record) {
      recordStore[record.id] = record;
    }

    function finalFlushCallback() {

      encoder.end(function () {        
        startAssertingScalableWrites(path, type, recordStore, batchRun, batchWrites, null, function() {
          if (batchRun < batches) {
            encoder = encoderBuilderFunction(path, type);
            batchRun++;
            scalableWrite(encoder, record, writeCallback, null, finalFlushCallback, batchRun, batchWrites);
          } else {
            cb();
          }
        });
      });
    }

    encoder = encoderBuilderFunction(path, type);
    scalableWrite(encoder, record, writeCallback, null, finalFlushCallback, batchRun, batchWrites);
  }

function startAssertingScalableWrites(path, type, recordStore, batchRun, batchWrites, onDataCallback, cb) {
    var runs = 0;

    function run() {      
      var n = 0;
      runs++;

      index.createFileDecoder(path)
        .on('data', function (obj) {
          var stored = recordStore[obj.id];
          n++;
          if (onDataCallback) { onDataCallback(obj) }
          assert(type.isValid(obj));          
          
          if (obj.id.indexOf('batch: ' + batchRun) >=0 ) {
            if (stored) {
              assert.deepEqual(obj, stored);
              delete recordStore[obj.id];
            }
          }
        })
        .on('end', function () {
          if (n < batchRun * batchWrites && runs < 3) {
            setTimeout(run, 10);
          } else {
            assert.equal(n, batchRun * batchWrites);
            assert.equal(0, Object.keys(recordStore).length);
            cb();
          }
        });
    }

    run();
}

function scalableWrite(writableStream, record, writeCallback, recordFlushCallback, finalFlushCallback, batchRun, batchWrites, offset) {
  var ok = true;
  var i = offset;

  if (offset === undefined) {
    i = batchWrites;
  }

  do {
    i--;
    var eventName = 'batch: ' + batchRun + ' event: ' + (batchWrites-i);
    var data = mergeObjects({}, record);
    
    assert.deepEqual(data, record);
    data.id = eventName;

    var flushCallback = function() {
      if (recordFlushCallback) recordFlushCallback(data);
    }
    var finalCallback = function() {
      flushCallback();
      if (finalFlushCallback) finalFlushCallback();
    }

    if (i > 0) {
      ok = writableStream.write(data, flushCallback);
    } else if (i === 0) {
      ok = writableStream.write(data, finalCallback);
    } else {
      return;
    }

    if (writeCallback) writeCallback(data);

    if (!ok) {
      writableStream.once('drain', function () {
        scalableWrite(writableStream, record, writeCallback, recordFlushCallback, finalFlushCallback, batchRun, batchWrites, i);
      });
    }

  } while (i > 0 && ok);
}

function mergeObjects() {
    var resObj = {};
    for(var i=0; i < arguments.length; i += 1) {
         var obj = arguments[i],
             keys = Object.keys(obj);
         for(var j=0; j < keys.length; j += 1) {
             resObj[keys[j]] = obj[keys[j]];
         }
    }
    return resObj;
}
