/* jshint node: true */

'use strict';

var avsc = require('../lib'),
    assert = require('assert'),
    fs = require('fs');


var schema = JSON.parse(fs.readFileSync('dat/event.avsc'));
var buf = fs.readFileSync('dat/event.avro');

new Benchmark()
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

/**
  * Benchmark functions serially.
  *
  * This is useful for async functions which yield too often to the event
  * loop to be correctly benchmarked.
  *
  */
function Benchmark() {

  var fns = {};

  /**
    * Add a function to be benchmarked.
    *
    * @param `name` A name to identify this function by.
    * @param `fn(cb, opts)` The function to be benchmarked. This function will
    * be passed two arguments: `cb([time])`, to be called when the function
    * completes (optionally passing an `hrtime` argument to override the
    * default time, e.g. to bypass setup and teardown durations), and the same
    * `opts` argument that was passed to the benchmark's `run` method.
    * @return `this`
    *
    */
  this.addFn = function (name, fn) {

    fns[name] = fn;
    return this;

  };

  /**
    * `run(n, [opts], cb)`
    *
    * Run all functions serially.
    *
    * @param `n` The number of runs per function.
    * @param `opts` Arguments to be passed to each benchmarked function (see
    * `addFn`).
    * @param `cb(stats)` Callback when the run completes.
    * @return `this`
    *
    */
  this.run = function (n, opts, cb) {

    if (!cb && typeof opts == 'function') {
      cb = opts;
      opts = {};
    }

    var names = Object.keys(fns);
    var stats = {};
    var i = 0;

    (function runCb(ts) {
      if (ts) {
        stats[names[i++]] = ts;
      }
      if (i < names.length) {
        setImmediate(function () { runFn(fns[names[i]], n, opts, runCb); });
      } else {
        cb(getRankedStats(stats));
      }
    })();

    return this;

  };

  function runFn(fn, n, opts, cb) {

    var times = [];
    var i = 0;
    var t;

    (function runCb(time) {
      if (i) {
        times.push(time || process.hrtime(t));
      }
      if (i++ < n) {
        t = process.hrtime();
        process.nextTick(function () { fn(runCb, opts); });
      } else {
        cb(getStats(times));
      }
    })();

    function getStats(ts) {
      var totMs = 0;
      for (var i = 0; i < ts.length; i++) {
        var time = ts[i];
        totMs += 1e3 * time[0] + 1e-6 * time[1];
      }
      var avgMs = totMs / ts.length;
      var varMs = 0;
      for (i = 0; i < ts.length; i++) {
        time = ts[i];
        varMs += Math.pow(1e3 * time[0] + 1e-6 * time[1] - avgMs, 2);
      }
      var sdvMs = Math.sqrt(varMs) / ts.length;
      return {avgMs: avgMs, sdvMs: sdvMs};
    }

  }

  function getRankedStats(stats) {

    var es = [];
    for (var name in stats) {
      var stat = stats[name];
      stat.name = name;
      es.push(stat);
    }

    es = es.sort(function (a, b) { return a.avgMs - b.avgMs; });

    var avg = es[0].avgMs;
    for (var i = 1; i < es.length; i++) {
      var e = es[i];
      e.relAvg = (e.avgMs - avg) / avg;
    }

    return es;

  }

}
