/* jshint node: true */

'use strict';

/**
 * Simple benchmarking entry point.
 *
 */

var avro = require('../../../lib'),
    Benchmark = require('benchmark'),
    commander = require('commander'),
    compactr = require('compactr'),
    flatbuffers = require('flatbuffers'),
    fs = require('fs'),
    msgpack = require('msgpack-lite'),
    Pbf = require('pbf'),
    pbCompile = require('pbf/compile'),
    pbSchema = require('protocol-buffers-schema'),
    protobuf = require('protocol-buffers'),
    protobufjs = require('protobufjs'),
    spack = require('schemapack'),
    util = require('util');


/**
 * Generate statistics for a given schema.
 *
 */
function generateStats(schema, opts) {
  opts = opts || {};

  var type = avro.parse(schema, {wrapUnions: opts.wrapUnions});
  return [DecodeSuite, EncodeSuite].map(function (Suite) {
    var stats = [];
    var suite = new Suite(type, opts)
      .on('start', function () { console.error(Suite.key_ + ' ' + type); })
      .on('cycle', function (evt) { console.error('' + evt.target); })
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
    return {name: Suite.key_, stats: stats};
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
  this._compatibleType = avro.parse(type.getSchema(), {
    typeHook: typeHook,
    wrapUnions: opts.wrapUnions
  });
  this._value = opts.value ? type.fromString(opts.value) : type.random();

  Object.keys(opts).forEach(function (name) {
    if (!name.indexOf('_')) {
      return;
    }
    var fn = this['__' + name];
    if (typeof fn == 'function') {
      this.add(name, fn.call(this, opts[name])); // Add benchmark.
    }
  }, this);
}
util.inherits(Suite, Benchmark.Suite);

Suite.prototype.getType = function (isProtobuf) {
  return isProtobuf ? this._compatibleType : this._type;
};

Suite.prototype.getValue = function (isProtobuf) {
  if (isProtobuf) {
    var type = this.getType(true); // Read enum values as integers.
    return type.fromBuffer(this.getType().toBuffer(this._value));
  } else {
    return this._value;
  }
};


/**
 * Basic decoding benchmark.
 *
 */
function DecodeSuite(type, opts) { Suite.call(this, type, opts); }
util.inherits(DecodeSuite, Suite);
DecodeSuite.key_ = 'decode';

DecodeSuite.prototype.__avsc = function () {
  var type = this.getType();
  var buf = type.toBuffer(this.getValue());
  return function () {
    var val = type.fromBuffer(buf);
    if (val.$) {
      throw new Error();
    }
  };
};

DecodeSuite.prototype.__compactr = function (args) {
  var schema = compactr.schema(JSON.parse(fs.readFileSync(args)));
  var buf = schema.write(this.getValue()).buffer();
  return function () {
    var obj = schema.read(buf);
    if (obj.$) {
      throw new Error();
    }
  };
};

DecodeSuite.prototype.__flatbuffers = function (args) {
  var root = flatbuffers.compileSchema(fs.readFileSync(args));
  var buf = Buffer.from(root.generate(this.getValue()));
  return function () {
    var obj = root.parse(buf);
    if (obj.$) {
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

DecodeSuite.prototype.__msgpackLite = function () {
  var buf = msgpack.encode(this.getValue());
  return function () {
    var obj = msgpack.decode(buf);
    if (obj.$) {
      throw new Error();
    }
  };
};

DecodeSuite.prototype.__pbf = function (args) {
  var parts = args.split(':');
  var proto = pbSchema.parse(fs.readFileSync(parts[0]));
  var message = pbCompile(proto)[parts[1]];
  var pbf = new Pbf();
  message.write(this.getValue(true), pbf);
  var buf = pbf.finish();
  return function () {
    var obj = message.read(new Pbf(buf));
    if (obj.$) {
      throw new Error();
    }
  };
};

DecodeSuite.prototype.__protobufjs = function (args) {
  var parts = args.split(':');
  var root = protobufjs.parse(fs.readFileSync(parts[0])).root;
  var message = root.lookup(parts[1]);
  var buf = message.encode(this.getValue(true)).finish();
  return function () {
    var obj = message.decode(buf);
    if (obj.$) {
      throw new Error();
    }
  };
};

DecodeSuite.prototype.__protocolBuffers = function (args) {
  var parts = args.split(':');
  var messages = protobuf(fs.readFileSync(parts[0]));
  var message = messages[parts[1]];
  var buf = message.encode(this.getValue(true));
  return function () {
    var obj = message.decode(buf);
    if (obj.$) {
      throw new Error();
    }
  };
};

DecodeSuite.prototype.__schemapack = function (args) {
  var schema = spack.build(JSON.parse(fs.readFileSync(args)));
  var buf = schema.encode(this.getValue(true));
  return function () {
    var obj = schema.decode(buf);
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

EncodeSuite.prototype.__avsc = function () {
  var type = this.getType();
  var val = this.getValue();
  return function () {
    var buf = type.toBuffer(val);
    if (!buf.length) {
      throw new Error();
    }
  };
};

EncodeSuite.prototype.__compactr = function (args) {
  var schema = compactr.schema(JSON.parse(fs.readFileSync(args)));
  var val = this.getValue();
  return function () {
    var buf = schema.write(val).buffer();
    if (!buf.length) {
      throw new Error();
    }
  };
};

EncodeSuite.prototype.__flatbuffers = function (args) {
  var message = flatbuffers.compileSchema(fs.readFileSync(args));
  var val = this.getValue(true);
  return function () {
    var buf = Buffer.from(message.generate(val).buffer);
    if (!buf.length) {
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

EncodeSuite.prototype.__msgpackLite = function () {
  var val = this.getValue();
  return function () {
    var buf = msgpack.encode(val);
    if (!buf.length) {
      throw new Error();
    }
  };
};

EncodeSuite.prototype.__pbf = function (args) {
  var parts = args.split(':');
  var proto = pbSchema.parse(fs.readFileSync(parts[0]));
  var message = pbCompile(proto)[parts[1]];
  var val = this.getValue(true);
  return function () {
    var pbf = new Pbf();
    message.write(val, pbf);
    var buf = pbf.finish();
    if (!buf.length) {
      throw new Error();
    }
  };
};

EncodeSuite.prototype.__protobufjs = function (args) {
  var parts = args.split(':');
  var root = protobufjs.parse(fs.readFileSync(parts[0])).root;
  var message = root.lookup(parts[1]);
  var val = this.getValue(true);
  return function () {
    var buf = message.encode(val).finish();
    if (!buf.length) {
      throw new Error();
    }
  };
};

EncodeSuite.prototype.__protocolBuffers = function (args) {
  var parts = args.split(':');
  var messages = protobuf(fs.readFileSync(parts[0]));
  var message = messages[parts[1]];
  var val = this.getValue(true);
  return function () {
    var buf = message.encode(val);
    if (!buf.length) {
      throw new Error();
    }
  };
};

EncodeSuite.prototype.__schemapack = function (args) {
  var schema = spack.build(JSON.parse(fs.readFileSync(args)));
  var val = this.getValue(true);
  return function () {
    var buf = schema.encode(val);
    if (!buf.length) {
      throw new Error();
    }
  };
};


commander
  .usage('[options] <schema>')
  .option('-v, --value <val>', 'Use this value for benchmarking.')
  .option('-w, --wrap-unions', 'Wrap unions.')
  .option('--avsc', 'Benchmark `avsc`.')
  .option('--compactr <path>', 'Benchmark `compactr`.')
  .option('--flatbuffers <path>', 'Benchmark `flatbuffers`.')
  .option('--json', 'Benchmark built-in JSON.')
  .option('--json-binary', 'Benchmark JSON (serializing bytes to strings).')
  .option('--json-string', 'Benchmark JSON (pre-parsing bytes to strings).')
  .option('--msgpack-lite', 'Benchmark `msgpack-lite`.')
  .option('--pbf <path:message>', 'Benchmark `pbf`.')
  .option('--protobufjs <path:message>', 'Benchmark `protobufjs`.')
  .option('--protocol-buffers <path:message>', 'Benchmark `protocol-buffers`.')
  .option('--schemapack <path>', 'Benchmark `schemapack`.')
  .parse(process.argv);

var schema = commander.args[0];
if (!schema) {
  console.error('Missing schema.');
  process.exit(1);
}

var stats = generateStats(schema, commander);
console.log(JSON.stringify(stats));

// Helpers.

/**
 * Typehook to represent enums as integers, required for `protocol-buffers`.
 *
 */
function typeHook(attrs, opts) {
  if (attrs.type === 'enum') {
    return avro.parse('int', opts);
  }
}
