/* jshint node: true */

'use strict';

var avsc = require('../../lib'),
    Benchmark = require('./benchmark'),
    assert = require('assert'),
    fs = require('fs');


var schema = JSON.parse(fs.readFileSync('dat/event.avsc'));
var buf = fs.readFileSync('dat/event.avro');

new Benchmark()
  .addFn('unwrapped', (function () {
    var type = avsc.parse(schema, {unwrapUnions: true});
    return function (cb) {
      var n = 0;
      var i, record;
      for (i = 0; i < 1000; i++) {
        record = type.decode(buf);
        n += record.header.memberId;
      }
      assert(n);
      cb();
    };
  })())
  .addFn('unwrapped from record', (function () {
    var type = avsc.parse(schema, {unwrapUnions: true});
    var Record = type.getRecordConstructor();
    return function (cb) {
      var n = 0;
      var i, record;
      for (i = 0; i < 1000; i++) {
        record = Record.decode(buf);
        n += record.header.memberId;
      }
      assert(n);
      cb();
    };
  })())
  .addFn('unwrapped checked', (function () {
    var type = avsc.parse(schema, {unwrapUnions: true});
    var Record = type.getRecordConstructor();
    return function (cb) {
      var i, record;
      for (i = 0; i < 1000; i++) {
        record = Record.decode(buf);
        assert(record.$isValid());
      }
      cb();
    };
  })())
  .addFn('wrapped', (function () {
    var type = avsc.parse(schema);
    return function (cb) {
      var n = 0;
      var i, record;
      for (i = 0; i < 1000; i++) {
        record = type.decode(buf);
        n += record.header.memberId;
      }
      assert(n);
      cb();
    };
  })())
  .addFn('from json', (function () {
    var type = avsc.parse(schema, {unwrapUnions: true});
    var s = JSON.stringify(type.decode(buf));
    return function (cb) {
      var n = 0;
      var i, record;
      for (i = 0; i < 1000; i++) {
        record = JSON.parse(s);
        n += record.header.memberId;
      }
      assert(n);
      cb();
    };
  })())
  .run(100,  function (stats) { console.dir(stats); });
