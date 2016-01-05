/* jshint node: true */

// TODO: Add support for protobufjs.

'use strict';

/**
 * Simple benchmarking entry point.
 *
 */

var avsc = require('../../lib'),
    Benchmark = require('benchmark'),
    commander = require('commander'),
    msgpack = require('msgpack-lite'),
    util = require('util');


/**
 * Generate statistics for a given schema.
 *
 */
function generateStats(schema, opts) {
  opts = opts || {};

  var type = avsc.parse(schema);
  return [DecodeSuite, EncodeSuite].map(function (Suite) {
    var stats = [];
    var l = opts.numValues || 1;
    var i = 0;
    var suite;
    while (i++ < l) {
      suite = new Suite(type, opts);
      suite
        .on('start', onStart)
        .on('cycle', onCycle)
        .run();
      stats.push({
        value: suite.getValue(),
        stats: suite.map(function (benchmark) {
          var stats = benchmark.stats;
          return {
            name: benchmark.name,
            mean: stats.mean,
            rme: stats.rme
          };
        })
      });
    }
    return {name: Suite.key_, stats: stats};

    function onStart() { console.error(Suite.key_ + ' ' + type + ' #' + i); }
    function onCycle(evt) { console.error('' + evt.target); }
  });
}


/**
 * Custom benchmark suite.
 *
 */
function Suite(type, opts) {
  Benchmark.Suite.call(this);

  opts = opts || {};
  this._type = type;
  this._value = opts.value ? type.fromString(opts.value) : type.random();

  Object.keys(opts).forEach(function (name) {
    if (!name.indexOf('_')) {
      return;
    }
    var fn = this['__' + name];
    if (typeof fn == 'function') {
      this.add(name, fn.call(this, opts.name)); // Add benchmark.
    }
  }, this);
}
util.inherits(Suite, Benchmark.Suite);
Suite.prototype.getType = function () { return this._type; };
Suite.prototype.getValue = function () { return this._value; };


/**
 * Basic decoding benchmark.
 *
 */
function DecodeSuite(type, opts) { Suite.call(this, type, opts); }
util.inherits(DecodeSuite, Suite);
DecodeSuite.key_ = 'decode';

DecodeSuite.prototype.__avro = function () {
  var type = this.getType();
  var buf = type.toBuffer(this.getValue());
  return function () {
    var val = type.fromBuffer(buf);
    if (val.$) {
      throw new Error();
    }
  };
};

DecodeSuite.prototype.__avroJson = function () {
  var type = this.getType();
  var str = type.toString(this.getValue());
  return function () {
    var val = type.fromString(str);
    if (val.$) {
      throw new Error();
    }
  };
};

DecodeSuite.prototype.__json = function () {
  var str = JSON.stringify(this.getValue());
  return function () {
    var obj = JSON.parse(str);
    if (obj.$) {
      throw new Error();
    }
  };
};

DecodeSuite.prototype.__jsonString = function () {
  var type = this.getType();
  var str = type.toString(this.getValue());
  return function () {
    var obj = JSON.parse(str);
    if (obj.$) {
      throw new Error();
    }
  };
};

DecodeSuite.prototype.__jsonBinary = function () {
  var str = JSON.stringify(this.getValue());
  return function () {
    var obj = JSON.parse(str, function (key, value) {
      return (value && value.type === 'Buffer') ? new Buffer(value) : value;
    });
    if (obj.$) {
      throw new Error();
    }
  };
};

DecodeSuite.prototype.__msgpack = function () {
  var buf = msgpack.encode(this.getValue());
  return function () {
    var obj = msgpack.decode(buf);
    if (obj.$) {
      throw new Error();
    }
  };
};


/**
 * Basic encoding benchmark.
 *
 */
function EncodeSuite(type, opts) { Suite.call(this, type, opts); }
util.inherits(EncodeSuite, Suite);
EncodeSuite.key_ = 'encode';

EncodeSuite.prototype.__avro = function () {
  var type = this.getType();
  var val = this.getValue();
  return function () {
    var buf = type.toBuffer(val);
    if (!buf.length) {
      throw new Error();
    }
  };
};

EncodeSuite.prototype.__avroJson = function () {
  var type = this.getType();
  var val = this.getValue();
  return function () {
    var str = type.toString(val);
    if (!str.length) {
      throw new Error();
    }
  };
};

EncodeSuite.prototype.__json = function () {
  var val = this.getValue();
  return function () {
    var str = JSON.stringify(val);
    if (!str.length) {
      throw new Error();
    }
  };
};

EncodeSuite.prototype.__jsonBinary = function () {
  var val = this.getValue();
  return function () {
    var str = JSON.stringify(val, function (key, value) {
      if (Buffer.isBuffer(value)) {
        return value.toString('binary');
      }
      return value;
    });
    if (!str.length) {
      throw new Error();
    }
  };
};

EncodeSuite.prototype.__jsonString = function () {
  var type = this.getType();
  var obj = JSON.parse(type.toString(this.getValue()));
  return function () {
    var str = JSON.stringify(obj);
    if (!str.length) {
      throw new Error();
    }
  };
};

EncodeSuite.prototype.__msgpack = function () {
  var val = this.getValue();
  return function () {
    var buf = msgpack.encode(val);
    if (!buf.length) {
      throw new Error();
    }
  };
};


commander
  .usage('[options] <schema>')
  .option('-v, --value <val>', 'Use this value for benchmarking.')
  .option('--avro', 'Benchmark `avsc`.')
  .option('--avro-json', 'Benchmark `avsc` (JSON serialization).')
  .option('--json', 'Benchmark JSON.')
  .option('--json-binary', 'Benchmark JSON (serializing bytes to strings).')
  .option('--json-string', 'Benchmark JSON (pre-parsing bytes to strings).')
  .option('--msgpack', 'Benchmark `msgpack-lite`.')
  .option('--protobuf <schema>', 'Benchmark `protobufjs`.')
  .parse(process.argv);

var schema = commander.args[0];
if (!schema) {
  console.error('Missing schema.');
  process.exit(1);
}

var stats = generateStats(schema, commander);
console.log(JSON.stringify(stats));
